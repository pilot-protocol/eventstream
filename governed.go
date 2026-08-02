// SPDX-License-Identifier: AGPL-3.0-or-later

package eventstream

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/pilot-protocol/common/coreapi"
	"github.com/pilot-protocol/common/decision"
)

// GovernedTopic is reserved for a signed publication envelope. The broker
// unwraps it only after local verification and forwards the original topic and
// payload to subscribers; subscribers never receive this transport topic.
const GovernedTopic = "\x00pilot.governed.v1"

const governedEventVersion uint16 = 1

// GovernedEvent transports one published Event with its exact signed intent
// and authority decision. Its payload binding includes both topic and bytes,
// so neither can be changed after authorization.
type GovernedEvent struct {
	Version    uint16                      `json:"version"`
	Topic      string                      `json:"topic"`
	Payload    []byte                      `json:"payload"`
	Disclosure *decision.DisclosureBinding `json:"disclosure,omitempty"`
	Intent     decision.Intent             `json:"intent"`
	Decision   decision.Decision           `json:"decision"`
}

// GovernedEventVerifier verifies a governed publication against the local
// broker's authority state before it is distributed to subscribers.
type GovernedEventVerifier interface {
	VerifyGovernedEvent(context.Context, coreapi.Addr, GovernedEvent) error
}

// GovernedReceiptRecorder durably records a broker-side enforcement receipt
// for a verified governed publication before it is fanned out to subscribers.
// Required enterprise deployments treat a recorder failure as a publication
// denial, so an event is never delivered without its evidence.
type GovernedReceiptRecorder interface {
	RecordGovernedReceipt(context.Context, decision.Intent, decision.Decision) error
}

// GovernedDisclosureReceiptRecorder records V2 evidence for typed governed
// publications. Brokers fail closed instead of recording a V1 receipt that
// omits disclosure proof.
type GovernedDisclosureReceiptRecorder interface {
	RecordGovernedDisclosureReceipt(context.Context, decision.Intent, decision.Decision, decision.DisclosureBinding) error
}

func recordGovernedReceipt(ctx context.Context, recorder GovernedReceiptRecorder, intent decision.Intent, result decision.Decision, disclosure *decision.DisclosureBinding) error {
	if recorder == nil {
		return fmt.Errorf("eventstream: governed receipt recorder is not configured")
	}
	if disclosure == nil {
		return recorder.RecordGovernedReceipt(ctx, intent, result)
	}
	typed, supported := recorder.(GovernedDisclosureReceiptRecorder)
	if !supported {
		return fmt.Errorf("eventstream: governed receipt recorder does not support disclosure evidence")
	}
	return typed.RecordGovernedDisclosureReceipt(ctx, intent, result, *disclosure)
}

// DecisionEventVerifier is the reference broker-side verifier. Resource must
// return the exact local resource for the incoming publication (for example,
// "eventstream:alerts"). It prevents a valid decision for one topic from
// being replayed to another topic at the same broker.
type DecisionEventVerifier struct {
	Enforcer          *decision.Enforcer
	Resource          func(coreapi.Addr, *Event) string
	RequireDisclosure bool
}

func NewGovernedEvent(event *Event, intent decision.Intent, result decision.Decision) (GovernedEvent, error) {
	if event == nil {
		return GovernedEvent{}, fmt.Errorf("eventstream: governed event is required")
	}
	governed := GovernedEvent{
		Version: governedEventVersion, Topic: event.Topic, Payload: append([]byte(nil), event.Payload...), Intent: intent, Decision: result,
	}
	if err := governed.Validate(); err != nil {
		return GovernedEvent{}, err
	}
	return governed, nil
}

// NewGovernedEventWithDisclosure creates a governed event whose Intent binds
// canonical disclosure metadata. The caller is responsible for requesting a
// Decision for that exact disclosure-bound Intent before publication.
func NewGovernedEventWithDisclosure(event *Event, intent decision.Intent, result decision.Decision, disclosure decision.DisclosureBinding) (GovernedEvent, error) {
	if event == nil {
		return GovernedEvent{}, fmt.Errorf("eventstream: governed event is required")
	}
	disclosure.Labels = append([]string(nil), disclosure.Labels...)
	governed := GovernedEvent{
		Version: governedEventVersion, Topic: event.Topic, Payload: append([]byte(nil), event.Payload...), Disclosure: &disclosure, Intent: intent, Decision: result,
	}
	if err := governed.Validate(); err != nil {
		return GovernedEvent{}, err
	}
	return governed, nil
}

func (event GovernedEvent) Validate() error {
	if event.Version != governedEventVersion || !validGovernedTopic(event.Topic) {
		return fmt.Errorf("eventstream: invalid governed topic")
	}
	if len(event.Payload) > 1<<24 {
		return fmt.Errorf("eventstream: governed payload exceeds maximum event size")
	}
	if err := event.Intent.Validate(); err != nil {
		return fmt.Errorf("eventstream: invalid governed intent: %w", err)
	}
	if event.Intent.Signature == "" || event.Decision.Signature == "" {
		return fmt.Errorf("eventstream: governed intent and decision must be signed")
	}
	if event.Intent.Action != "event.publish" {
		return fmt.Errorf("eventstream: governed intent action must be event.publish")
	}
	if err := event.verifyPayloadBinding(); err != nil {
		return err
	}
	if err := event.Decision.Validate(); err != nil {
		return fmt.Errorf("eventstream: invalid governed decision: %w", err)
	}
	return nil
}

func (event GovernedEvent) verifyPayloadBinding() error {
	if event.Disclosure == nil {
		if event.Intent.PayloadHash != GovernedEventPayloadHash(event.Topic, event.Payload) {
			return fmt.Errorf("eventstream: governed intent payload binding mismatch")
		}
		return nil
	}
	if event.Disclosure.ContentHash != decision.HashPayload(event.Payload) || event.Disclosure.DeclaredBytes != uint64(len(event.Payload)) || event.Disclosure.Filename != "" || event.Disclosure.TransferID != "" {
		return fmt.Errorf("eventstream: governed disclosure does not match event")
	}
	if err := event.Disclosure.VerifyIntent(event.Intent); err != nil {
		return fmt.Errorf("eventstream: governed disclosure intent binding: %w", err)
	}
	return nil
}

func (event GovernedEvent) Event() *Event {
	return &Event{Topic: event.Topic, Payload: append([]byte(nil), event.Payload...)}
}

func EncodeGovernedEvent(event GovernedEvent) (*Event, error) {
	if err := event.Validate(); err != nil {
		return nil, err
	}
	body, err := json.Marshal(event)
	if err != nil {
		return nil, fmt.Errorf("eventstream: encode governed event: %w", err)
	}
	if len(body) > 1<<24 {
		return nil, fmt.Errorf("eventstream: governed envelope exceeds maximum event size")
	}
	return &Event{Topic: GovernedTopic, Payload: body}, nil
}

func DecodeGovernedEvent(event *Event) (GovernedEvent, error) {
	if event == nil || event.Topic != GovernedTopic || len(event.Payload) > 1<<24 {
		return GovernedEvent{}, fmt.Errorf("eventstream: invalid governed envelope")
	}
	decoder := json.NewDecoder(bytes.NewReader(event.Payload))
	decoder.DisallowUnknownFields()
	var governed GovernedEvent
	if err := decoder.Decode(&governed); err != nil {
		return GovernedEvent{}, fmt.Errorf("eventstream: decode governed envelope: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return GovernedEvent{}, fmt.Errorf("eventstream: trailing governed envelope data")
	}
	if err := governed.Validate(); err != nil {
		return GovernedEvent{}, err
	}
	return governed, nil
}

func (verifier DecisionEventVerifier) VerifyGovernedEvent(ctx context.Context, remote coreapi.Addr, governed GovernedEvent) error {
	if verifier.Enforcer == nil || verifier.Resource == nil {
		return fmt.Errorf("eventstream: decision event verifier is not initialized")
	}
	if err := governed.Validate(); err != nil {
		return err
	}
	if verifier.RequireDisclosure && governed.Disclosure == nil {
		return fmt.Errorf("eventstream: governed disclosure is required")
	}
	event := governed.Event()
	resource := verifier.Resource(remote, event)
	if resource == "" || governed.Intent.Resource != resource {
		return fmt.Errorf("eventstream: governed intent resource binding mismatch")
	}
	var verifyErr error
	if governed.Disclosure != nil {
		verifyErr = verifier.Enforcer.VerifyDisclosure(ctx, governed.Intent, governed.Decision, *governed.Disclosure)
	} else {
		verifyErr = verifier.Enforcer.Verify(ctx, governed.Intent, governed.Decision)
	}
	if verifyErr != nil {
		return fmt.Errorf("eventstream: verify governed decision: %w", verifyErr)
	}
	switch governed.Decision.Outcome {
	case decision.Allow:
		return nil
	case decision.Constrain:
		return enforceEventConstraints(governed.Decision.Constraints, remote, event)
	default:
		return fmt.Errorf("eventstream: governed decision outcome %q cannot permit publication", governed.Decision.Outcome)
	}
}

// GovernedEventPayloadHash returns the exact payload binding required by a
// GovernedTopic Intent. It includes topic and bytes, so callers must use it
// when creating the Intent before PublishGoverned.
func GovernedEventPayloadHash(topic string, payload []byte) string {
	var header [8]byte
	binary.BigEndian.PutUint32(header[:4], uint32(len(topic)))
	binary.BigEndian.PutUint32(header[4:8], uint32(len(payload)))
	hash := sha256.New()
	_, _ = hash.Write([]byte("pilot-eventstream-governed-event-v1\x00"))
	_, _ = hash.Write(header[:])
	_, _ = hash.Write([]byte(topic))
	_, _ = hash.Write(payload)
	return decision.HashPayload(hash.Sum(nil))
}

func validGovernedTopic(topic string) bool {
	return topic != "" && topic != GovernedTopic && len(topic) <= 1024 && utf8.ValidString(topic)
}

func enforceEventConstraints(constraints []decision.Constraint, remote coreapi.Addr, event *Event) error {
	attributes := map[string]string{
		"publisher": remote.String(), "topic": event.Topic, "bytes": strconv.Itoa(len(event.Payload)),
	}
	for _, constraint := range constraints {
		actual, found := attributes[constraint.Key]
		if !found {
			return fmt.Errorf("eventstream: constraint %q has no enforceable event attribute", constraint.Key)
		}
		switch constraint.Operator {
		case "eq":
			if actual != constraint.Value {
				return fmt.Errorf("eventstream: constraint %s rejected", constraint.Key)
			}
		case "one_of":
			matched := false
			for _, allowed := range strings.Split(constraint.Value, ",") {
				if actual == strings.TrimSpace(allowed) {
					matched = true
					break
				}
			}
			if !matched {
				return fmt.Errorf("eventstream: constraint %s rejected", constraint.Key)
			}
		case "max", "min":
			value, valueErr := strconv.ParseUint(actual, 10, 64)
			limit, limitErr := strconv.ParseUint(constraint.Value, 10, 64)
			if valueErr != nil || limitErr != nil || (constraint.Operator == "max" && value > limit) || (constraint.Operator == "min" && value < limit) {
				return fmt.Errorf("eventstream: numeric constraint %s rejected", constraint.Key)
			}
		case "require":
			if constraint.Value != "" && actual != constraint.Value {
				return fmt.Errorf("eventstream: required constraint %s rejected", constraint.Key)
			}
		default:
			return fmt.Errorf("eventstream: constraint operator %q is not enforceable", constraint.Operator)
		}
	}
	return nil
}
