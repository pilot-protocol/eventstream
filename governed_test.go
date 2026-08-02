// SPDX-License-Identifier: AGPL-3.0-or-later

//go:build !no_eventstream
// +build !no_eventstream

package eventstream

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/pilot-protocol/common/coreapi"
	"github.com/pilot-protocol/common/decision"
)

type governedEventTestTrust struct {
	intentKey   ed25519.PublicKey
	decisionKey ed25519.PublicKey
}

func (trust governedEventTestTrust) IntentKey(context.Context, string, string, string) (ed25519.PublicKey, error) {
	return trust.intentKey, nil
}

func (trust governedEventTestTrust) DecisionKey(context.Context, string, string) (ed25519.PublicKey, error) {
	return trust.decisionKey, nil
}

func (governedEventTestTrust) MinimumState(context.Context, string) (uint64, uint64, error) {
	return 7, 3, nil
}

type governedEventTestCeiling struct{}

func (governedEventTestCeiling) Check(context.Context, decision.Intent, decision.Decision) error {
	return nil
}

func (governedEventTestCeiling) CheckDisclosure(context.Context, decision.Intent, decision.Decision, decision.DisclosureBinding) error {
	return nil
}

type governedEventReceiptRecorder struct {
	calls    int
	intent   decision.Intent
	decision decision.Decision
	err      error
}

type eventContentInspectorFunc func(context.Context, decision.Intent, *decision.DisclosureBinding, string, string, io.Reader) error

func (inspect eventContentInspectorFunc) InspectDisclosureContent(ctx context.Context, intent decision.Intent, disclosure *decision.DisclosureBinding, contentType, filename string, content io.Reader) error {
	return inspect(ctx, intent, disclosure, contentType, filename, content)
}

func TestGovernedPublicationContentInspectionRunsBeforeFanout(t *testing.T) {
	governed, verifier := newGovernedEventForTest(t, &Event{Topic: "alerts", Payload: []byte("classified")}, decision.Allow, nil)
	envelope, err := EncodeGovernedEvent(governed)
	if err != nil {
		t.Fatal(err)
	}
	senderServer, senderClient := newPipeStreamPair()
	defer senderClient.Close()
	broker := newBroker(nil, defaultAllowPolicy{})
	broker.governedVerifier = verifier
	var observed []byte
	broker.contentInspector = eventContentInspectorFunc(func(_ context.Context, intent decision.Intent, disclosure *decision.DisclosureBinding, contentType, filename string, content io.Reader) error {
		if intent.ID != governed.Intent.ID || disclosure != nil || contentType != "application/octet-stream" || filename != "" {
			t.Fatalf("inspection metadata intent=%+v disclosure=%+v content_type=%q filename=%q", intent, disclosure, contentType, filename)
		}
		var readErr error
		observed, readErr = io.ReadAll(content)
		return readErr
	})
	published, err := broker.governPublication(newSubscriber(senderServer), envelope)
	if err != nil || published == nil || !bytes.Equal(observed, governed.Payload) {
		t.Fatalf("inspection published=%+v err=%v payload=%q", published, err, observed)
	}
	broker.contentInspector = eventContentInspectorFunc(func(context.Context, decision.Intent, *decision.DisclosureBinding, string, string, io.Reader) error {
		return errors.New("scanner unavailable")
	})
	if _, err := broker.governPublication(newSubscriber(senderServer), envelope); err == nil || err.Error() != "eventstream: governed content inspection rejected" {
		t.Fatalf("inspection failure leaked or was accepted: %v", err)
	}
}

func TestGovernedPublicationQuotaUsesSignedAgentIdentity(t *testing.T) {
	governed, verifier := newGovernedEventForTest(t, &Event{Topic: "alerts", Payload: []byte("12345")}, decision.Allow, nil)
	envelope, err := EncodeGovernedEvent(governed)
	if err != nil {
		t.Fatal(err)
	}
	limiter, err := decision.NewTransferQuotaLimiter(decision.TransferQuotaConfig{Window: time.Minute, MaxBytes: 5, MaxSenders: 2})
	if err != nil {
		t.Fatal(err)
	}
	senderServer, senderClient := newPipeStreamPair()
	defer senderClient.Close()
	broker := newBroker(nil, defaultAllowPolicy{})
	broker.governedVerifier = verifier
	broker.governedTransferQuota = limiter
	if _, err := broker.governPublication(newSubscriber(senderServer), envelope); err != nil {
		t.Fatal(err)
	}
	if _, err := broker.governPublication(newSubscriber(senderServer), envelope); err == nil || err.Error() != "eventstream: governed transfer quota rejected" {
		t.Fatalf("quota error=%v", err)
	}
}

func (recorder *governedEventReceiptRecorder) RecordGovernedReceipt(_ context.Context, intent decision.Intent, result decision.Decision) error {
	recorder.calls++
	recorder.intent, recorder.decision = intent, result
	return recorder.err
}

type legacyGovernedEventReceiptRecorder struct{}

func (legacyGovernedEventReceiptRecorder) RecordGovernedReceipt(context.Context, decision.Intent, decision.Decision) error {
	return nil
}

type disclosureGovernedEventReceiptRecorder struct {
	governedEventReceiptRecorder
	disclosure decision.DisclosureBinding
}

func (recorder *disclosureGovernedEventReceiptRecorder) RecordGovernedDisclosureReceipt(_ context.Context, intent decision.Intent, result decision.Decision, disclosure decision.DisclosureBinding) error {
	recorder.calls++
	recorder.intent, recorder.decision, recorder.disclosure = intent, result, disclosure
	return recorder.err
}

func TestDisclosurePublicationReceiptRecorderRequiresV2Evidence(t *testing.T) {
	disclosure := decision.DisclosureBinding{Version: decision.DisclosureBindingVersion}
	if err := recordGovernedReceipt(context.Background(), legacyGovernedEventReceiptRecorder{}, decision.Intent{ID: "intent"}, decision.Decision{ID: "decision"}, &disclosure); err == nil || !strings.Contains(err.Error(), "does not support disclosure") {
		t.Fatalf("legacy disclosure recorder err=%v", err)
	}
	recorder := &disclosureGovernedEventReceiptRecorder{}
	if err := recordGovernedReceipt(context.Background(), recorder, decision.Intent{ID: "intent"}, decision.Decision{ID: "decision"}, &disclosure); err != nil {
		t.Fatal(err)
	}
	if recorder.calls != 1 || recorder.disclosure.Version != decision.DisclosureBindingVersion {
		t.Fatalf("disclosure recorder=%+v", recorder)
	}
}

type verifierStub struct{}

func (verifierStub) VerifyGovernedEvent(context.Context, coreapi.Addr, GovernedEvent) error {
	return nil
}

func newGovernedEventForTest(t *testing.T, event *Event, outcome decision.Outcome, constraints []decision.Constraint) (GovernedEvent, DecisionEventVerifier) {
	t.Helper()
	intentPublic, intentPrivate, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate intent key: %v", err)
	}
	decisionPublic, decisionPrivate, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate decision key: %v", err)
	}
	now := time.Now().UTC().Truncate(time.Second)
	nonce, err := decision.NewNonce()
	if err != nil {
		t.Fatalf("nonce: %v", err)
	}
	intent := decision.Intent{
		Version: decision.SchemaVersion, ID: "event-intent", TenantID: "tenant-a", AgentID: "publisher-a",
		Action: "event.publish", Resource: "eventstream:" + event.Topic, PayloadHash: GovernedEventPayloadHash(event.Topic, event.Payload),
		Risk: decision.RiskMedium, IssuedAt: now.Unix(), ExpiresAt: now.Add(2 * time.Minute).Unix(), Nonce: nonce, KeyID: "publisher-key",
	}
	if err := intent.Sign(intentPrivate); err != nil {
		t.Fatalf("sign intent: %v", err)
	}
	intentHash, err := intent.Hash()
	if err != nil {
		t.Fatalf("hash intent: %v", err)
	}
	result := decision.Decision{
		Version: decision.SchemaVersion, ID: "event-decision", IntentHash: intentHash, TenantID: intent.TenantID, AgentID: intent.AgentID,
		Outcome: outcome, Constraints: constraints, PolicyRevision: 7, RevocationEpoch: 3, ProviderID: "authority-a",
		IssuedAt: now.Unix(), ExpiresAt: now.Add(90 * time.Second).Unix(), KeyID: "authority-key",
	}
	if err := result.Sign(decisionPrivate); err != nil {
		t.Fatalf("sign decision: %v", err)
	}
	governed, err := NewGovernedEvent(event, intent, result)
	if err != nil {
		t.Fatalf("new governed event: %v", err)
	}
	verifier := DecisionEventVerifier{
		Enforcer: &decision.Enforcer{
			Trust: governedEventTestTrust{intentKey: intentPublic, decisionKey: decisionPublic}, Ceiling: governedEventTestCeiling{}, Now: func() time.Time { return now },
		},
		Resource: func(_ coreapi.Addr, event *Event) string { return "eventstream:" + event.Topic },
	}
	return governed, verifier
}

func TestGovernedEventRoundTripBindsTopicAndPayload(t *testing.T) {
	governed, _ := newGovernedEventForTest(t, &Event{Topic: "alerts", Payload: []byte("approved alert")}, decision.Allow, nil)
	envelope, err := EncodeGovernedEvent(governed)
	if err != nil {
		t.Fatalf("encode governed event: %v", err)
	}
	decoded, err := DecodeGovernedEvent(envelope)
	if err != nil {
		t.Fatalf("decode governed event: %v", err)
	}
	if got := decoded.Event(); got.Topic != "alerts" || string(got.Payload) != "approved alert" {
		t.Fatalf("decoded event = %#v", got)
	}

	decoded.Topic = "other-alerts"
	tampered, err := json.Marshal(decoded)
	if err != nil {
		t.Fatalf("marshal tampered event: %v", err)
	}
	if _, err := DecodeGovernedEvent(&Event{Topic: GovernedTopic, Payload: tampered}); err == nil || !strings.Contains(err.Error(), "payload binding") {
		t.Fatalf("tampered event error = %v, want payload binding failure", err)
	}

	invalidTopic := governed
	invalidTopic.Topic = string([]byte{0xff})
	if err := invalidTopic.Validate(); err == nil || !strings.Contains(err.Error(), "invalid governed topic") {
		t.Fatalf("invalid topic error = %v, want UTF-8 validation failure", err)
	}
}

func TestGovernedEventDisclosureBindingAndRequiredProfile(t *testing.T) {
	event := &Event{Topic: "finance.alerts", Payload: []byte(`{"amount":42}`)}
	governed, verifier := newGovernedEventForTest(t, event, decision.Allow, nil)
	strict := verifier
	strict.RequireDisclosure = true
	if err := strict.VerifyGovernedEvent(context.Background(), coreapi.Addr{}, governed); err == nil || !strings.Contains(err.Error(), "disclosure is required") {
		t.Fatalf("missing disclosure error=%v", err)
	}
	binding := decision.DisclosureBinding{
		Version: decision.DisclosureBindingVersion, ContentHash: decision.HashPayload(event.Payload), DeclaredBytes: uint64(len(event.Payload)),
		ContentType: "application/json", Labels: []string{"finance", "pii"}, Recipient: "broker:finance", Purpose: "alert-publication", Residency: "eu-west-1",
	}
	hash, err := binding.Hash()
	if err != nil {
		t.Fatal(err)
	}
	governed.Intent.PayloadHash = hash
	governed.Intent.Audience = binding.Recipient
	governed.Intent.Purpose = binding.Purpose
	governed.Intent.Signature = "transport-test-signature"
	governed.Decision.Signature = "transport-test-signature"
	governed, err = NewGovernedEventWithDisclosure(event, governed.Intent, governed.Decision, binding)
	if err != nil {
		t.Fatalf("valid disclosure envelope: %v", err)
	}
	tampered := governed
	tampered.Disclosure = &binding
	tampered.Disclosure.Labels = []string{"finance"}
	if err := tampered.Validate(); err == nil || !strings.Contains(err.Error(), "disclosure intent binding") {
		t.Fatalf("disclosure mutation error=%v", err)
	}
}

func TestDecisionEventVerifierBindsBrokerAndEnforcesConstraints(t *testing.T) {
	governed, verifier := newGovernedEventForTest(t, &Event{Topic: "alerts", Payload: []byte("hello")}, decision.Constrain, []decision.Constraint{
		{Key: "topic", Operator: "eq", Value: "alerts"},
		{Key: "bytes", Operator: "max", Value: "5"},
	})
	if err := verifier.VerifyGovernedEvent(context.Background(), coreapi.Addr{}, governed); err != nil {
		t.Fatalf("verify governed event: %v", err)
	}

	wrongBroker := verifier
	wrongBroker.Resource = func(_ coreapi.Addr, _ *Event) string { return "eventstream:other" }
	if err := wrongBroker.VerifyGovernedEvent(context.Background(), coreapi.Addr{}, governed); err == nil || !strings.Contains(err.Error(), "resource binding") {
		t.Fatalf("wrong broker error = %v, want resource-binding failure", err)
	}

	tooLarge, constrainedVerifier := newGovernedEventForTest(t, &Event{Topic: "alerts", Payload: []byte("too large")}, decision.Constrain, []decision.Constraint{{Key: "bytes", Operator: "max", Value: "3"}})
	if err := constrainedVerifier.VerifyGovernedEvent(context.Background(), coreapi.Addr{}, tooLarge); err == nil || !strings.Contains(err.Error(), "numeric constraint") {
		t.Fatalf("oversize event error = %v, want constraint failure", err)
	}
}

func TestBrokerRequireGovernedUnwrapsVerifiedAndRejectsLegacy(t *testing.T) {
	governed, verifier := newGovernedEventForTest(t, &Event{Topic: "alerts", Payload: []byte("approved")}, decision.Allow, nil)
	envelope, err := EncodeGovernedEvent(governed)
	if err != nil {
		t.Fatalf("encode governed event: %v", err)
	}
	server, peer := newPipeStreamPair()
	defer server.Close()
	defer peer.Close()
	broker := newBroker(nil, defaultAllowPolicy{})
	broker.requireGoverned = true
	broker.governedVerifier = verifier
	sender := newSubscriber(server)

	got, err := broker.governPublication(sender, envelope)
	if err != nil {
		t.Fatalf("governed publication: %v", err)
	}
	if got.Topic != "alerts" || string(got.Payload) != "approved" {
		t.Fatalf("unwrapped publication = %#v", got)
	}
	if _, err := broker.governPublication(sender, &Event{Topic: "alerts", Payload: []byte("legacy")}); err == nil || !strings.Contains(err.Error(), "unsigned legacy") {
		t.Fatalf("legacy publication error = %v, want governed rejection", err)
	}
}

func TestBrokerHandleConnRequireGovernedDropsLegacyPublication(t *testing.T) {
	bus := &stubEventBus{}
	governed, verifier := newGovernedEventForTest(t, &Event{Topic: "alerts", Payload: []byte("approved")}, decision.Allow, nil)
	if _, err := EncodeGovernedEvent(governed); err != nil {
		t.Fatalf("governed fixture must encode: %v", err)
	}
	broker := newBroker(bus, defaultAllowPolicy{})
	broker.requireGoverned = true
	broker.governedVerifier = verifier
	server, client := newPipeStreamPair()
	sender := newSubscriber(server)
	done := make(chan struct{})
	go func() {
		defer close(done)
		broker.handleConn(sender)
	}()

	if err := WriteEvent(client, &Event{Topic: "publisher"}); err != nil {
		t.Fatalf("write subscription: %v", err)
	}
	if err := WriteEvent(client, &Event{Topic: "alerts", Payload: []byte("legacy")}); err != nil {
		t.Fatalf("write legacy publication: %v", err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && !bus.seen("pubsub.publish_denied") {
		time.Sleep(5 * time.Millisecond)
	}
	if !bus.seen("pubsub.publish_denied") {
		t.Fatalf("expected pubsub.publish_denied, got %v", bus.topics)
	}
	_ = client.Close()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("broker did not return after client close")
	}
}

func TestBrokerRecordsGovernedReceiptBeforeFanout(t *testing.T) {
	bus := &stubEventBus{}
	governed, verifier := newGovernedEventForTest(t, &Event{Topic: "alerts", Payload: []byte("approved")}, decision.Allow, nil)
	envelope, err := EncodeGovernedEvent(governed)
	if err != nil {
		t.Fatal(err)
	}
	recorder := &governedEventReceiptRecorder{}
	broker := newBroker(bus, defaultAllowPolicy{})
	broker.requireGoverned, broker.governedVerifier = true, verifier
	broker.requireReceipts, broker.receiptRecorder = true, recorder

	senderServer, senderClient := newPipeStreamPair()
	defer senderClient.Close()
	sender := newSubscriber(senderServer)
	receiverServer, receiverClient := newPipeStreamPair()
	defer receiverClient.Close()
	if !broker.addSub("alerts", newSubscriber(receiverServer)) {
		t.Fatal("add receiver")
	}
	received := make(chan *Event, 1)
	go func() {
		event, _ := ReadEvent(receiverClient)
		received <- event
	}()
	done := make(chan struct{})
	go func() {
		defer close(done)
		broker.handleConn(sender)
	}()
	if err := WriteEvent(senderClient, &Event{Topic: "publisher"}); err != nil {
		t.Fatal(err)
	}
	if err := WriteEvent(senderClient, envelope); err != nil {
		t.Fatal(err)
	}
	select {
	case event := <-received:
		if event == nil || event.Topic != "alerts" || string(event.Payload) != "approved" {
			t.Fatalf("fanout event=%+v", event)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("governed event was not fanned out")
	}
	if recorder.calls != 1 || recorder.intent.ID != governed.Intent.ID || recorder.decision.ID != governed.Decision.ID {
		t.Fatalf("receipt recorder=%+v", recorder)
	}
	_ = senderClient.Close()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("broker did not stop after publisher close")
	}
}

func TestBrokerReceiptFailurePreventsGovernedFanout(t *testing.T) {
	bus := &stubEventBus{}
	governed, verifier := newGovernedEventForTest(t, &Event{Topic: "alerts", Payload: []byte("approved")}, decision.Allow, nil)
	envelope, err := EncodeGovernedEvent(governed)
	if err != nil {
		t.Fatal(err)
	}
	broker := newBroker(bus, defaultAllowPolicy{})
	broker.requireGoverned, broker.governedVerifier = true, verifier
	broker.requireReceipts, broker.receiptRecorder = true, &governedEventReceiptRecorder{err: errors.New("journal unavailable")}
	senderServer, senderClient := newPipeStreamPair()
	sender := newSubscriber(senderServer)
	done := make(chan struct{})
	go func() {
		defer close(done)
		broker.handleConn(sender)
	}()
	if err := WriteEvent(senderClient, &Event{Topic: "publisher"}); err != nil {
		t.Fatal(err)
	}
	if err := WriteEvent(senderClient, envelope); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && !bus.seen("pubsub.publish_denied") {
		time.Sleep(5 * time.Millisecond)
	}
	if !bus.seen("pubsub.publish_denied") || bus.seen("pubsub.published") {
		t.Fatal("receipt failure did not deny publication before fanout")
	}
	_ = senderClient.Close()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("broker did not stop after publisher close")
	}
}

func TestServiceRequireGovernedNeedsVerifier(t *testing.T) {
	service := NewService()
	service.SetGovernedPublication(nil, true)
	if err := service.Start(context.Background(), coreapi.Deps{}); err == nil || !strings.Contains(err.Error(), "no verifier") {
		t.Fatalf("start error = %v, want missing verifier", err)
	}
	service = NewService()
	service.SetGovernedPublication(verifierStub{}, true)
	service.SetGovernedReceiptRecorder(nil, true)
	if err := service.Start(context.Background(), coreapi.Deps{}); err == nil || !strings.Contains(err.Error(), "receipt recorder") {
		t.Fatalf("start error = %v, want missing receipt recorder", err)
	}
}

var _ decision.TrustStore = governedEventTestTrust{}
var _ decision.AuthorityCeiling = governedEventTestCeiling{}
var _ GovernedReceiptRecorder = (*governedEventReceiptRecorder)(nil)
