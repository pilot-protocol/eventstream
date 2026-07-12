// SPDX-License-Identifier: AGPL-3.0-or-later

//go:build !no_eventstream
// +build !no_eventstream

package eventstream

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/pilot-protocol/common/coreapi"
)

// --- stub EventBus -----------------------------------------------------------

// stubEventBus records every Publish call so tests can assert that the
// broker emitted "pubsub.subscribed", "pubsub.unsubscribed",
// "pubsub.published", and "pubsub.rate_limited" events on the right
// branches.
type stubEventBus struct {
	mu     sync.Mutex
	topics []string
	events []map[string]any
}

func (s *stubEventBus) Publish(topic string, payload map[string]any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.topics = append(s.topics, topic)
	s.events = append(s.events, payload)
}

func (s *stubEventBus) Subscribe(pattern string) (<-chan coreapi.Event, func()) {
	ch := make(chan coreapi.Event)
	return ch, func() {}
}

func (s *stubEventBus) seen(topic string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, t := range s.topics {
		if t == topic {
			return true
		}
	}
	return false
}

func (s *stubEventBus) count(topic string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, t := range s.topics {
		if t == topic {
			n++
		}
	}
	return n
}

// --- net.Pipe-backed coreapi.Stream -----------------------------------------

// pipeStream adapts net.Pipe to coreapi.Stream so we can drive broker
// handleConn (which the service uses) directly without a real tunnel.
type pipeStream struct {
	c         net.Conn
	closeOnce sync.Once
}

func newPipeStreamPair() (*pipeStream, *pipeStream) {
	a, b := net.Pipe()
	return &pipeStream{c: a}, &pipeStream{c: b}
}

func (s *pipeStream) Read(p []byte) (int, error)  { return s.c.Read(p) }
func (s *pipeStream) Write(p []byte) (int, error) { return s.c.Write(p) }
func (s *pipeStream) Close() error {
	var err error
	s.closeOnce.Do(func() { err = s.c.Close() })
	return err
}
func (s *pipeStream) LocalAddr() coreapi.Addr          { return coreapi.Addr{} }
func (s *pipeStream) LocalPort() uint16                { return 1002 }
func (s *pipeStream) RemoteAddr() coreapi.Addr         { return coreapi.Addr{Node: 0xBEEF} }
func (s *pipeStream) RemotePort() uint16               { return 12345 }
func (s *pipeStream) SetDeadline(time.Time) error      { return nil }
func (s *pipeStream) SetReadDeadline(time.Time) error  { return nil }
func (s *pipeStream) SetWriteDeadline(time.Time) error { return nil }

// --- broker handleConn tests -------------------------------------------------

// TestBroker_HandleConn_SubscribeEmitsEvent drives handleConn through a
// successful subscribe and verifies the events bus saw "pubsub.subscribed"
// + later "pubsub.unsubscribed" on EOF.
func TestBroker_HandleConn_SubscribeEmitsEvent(t *testing.T) {
	t.Parallel()
	bus := &stubEventBus{}
	b := newBroker(bus, nil)

	subStream, clientEnd := newPipeStreamPair()
	sub := newSubscriber(subStream)

	done := make(chan struct{})
	go func() {
		defer close(done)
		b.handleConn(sub)
	}()

	// Client side writes the subscribe event.
	if err := WriteEvent(clientEnd, &Event{Topic: "alpha"}); err != nil {
		t.Fatalf("WriteEvent subscribe: %v", err)
	}

	// Wait for the broker goroutine to register the subscription. We
	// poll the broker subs map.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		b.mu.RLock()
		n := len(b.subs["alpha"])
		b.mu.RUnlock()
		if n == 1 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	if !bus.seen("pubsub.subscribed") {
		t.Fatalf("expected pubsub.subscribed event, got topics=%v", bus.topics)
	}

	// Closing the client end produces a read error in handleConn, which
	// triggers the deferred unsubscribe path.
	_ = clientEnd.Close()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("handleConn did not return after client close")
	}

	if !bus.seen("pubsub.unsubscribed") {
		t.Fatalf("expected pubsub.unsubscribed event on close, got topics=%v", bus.topics)
	}
}

// TestBroker_HandleConn_SubscribeReadFails covers the early-return path
// when the first ReadEvent fails (no subscribe event ever lands).
func TestBroker_HandleConn_SubscribeReadFails(t *testing.T) {
	t.Parallel()
	bus := &stubEventBus{}
	b := newBroker(bus, nil)

	subStream, clientEnd := newPipeStreamPair()
	sub := newSubscriber(subStream)

	done := make(chan struct{})
	go func() {
		defer close(done)
		b.handleConn(sub)
	}()

	// Close the client end before sending any event — ReadEvent returns
	// io.EOF (or io.ErrClosedPipe), broker hits the early-return branch.
	_ = clientEnd.Close()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("handleConn did not return after client close")
	}

	// topic was never set, so neither subscribed nor unsubscribed
	// should have been published.
	if bus.seen("pubsub.subscribed") {
		t.Error("did not expect pubsub.subscribed on early-fail path")
	}
	if bus.seen("pubsub.unsubscribed") {
		t.Error("did not expect pubsub.unsubscribed on early-fail path")
	}
}

// TestBroker_HandleConn_PublishedEventBusBranch covers the
// pubsub.published bus branch (a sender subscribes, then publishes one
// event with no receivers — the bus event still fires).
func TestBroker_HandleConn_PublishedEventBusBranch(t *testing.T) {
	t.Parallel()
	bus := &stubEventBus{}
	b := newBroker(bus, nil)

	subStream, clientEnd := newPipeStreamPair()
	sub := newSubscriber(subStream)

	done := make(chan struct{})
	go func() {
		defer close(done)
		b.handleConn(sub)
	}()

	// 1) subscribe (handshake)
	if err := WriteEvent(clientEnd, &Event{Topic: "topic-x"}); err != nil {
		t.Fatalf("WriteEvent subscribe: %v", err)
	}
	// Wait for subscription to be registered.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		b.mu.RLock()
		n := len(b.subs["topic-x"])
		b.mu.RUnlock()
		if n == 1 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	// 2) publish — no other subscribers, but pubsub.published should fire.
	if err := WriteEvent(clientEnd, &Event{Topic: "topic-x", Payload: []byte("hi")}); err != nil {
		t.Fatalf("WriteEvent publish: %v", err)
	}

	// Wait for the published bus event.
	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if bus.seen("pubsub.published") {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if !bus.seen("pubsub.published") {
		t.Fatalf("expected pubsub.published event, got %v", bus.topics)
	}

	_ = clientEnd.Close()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("handleConn did not return")
	}
}

// TestBroker_HandleConn_RateLimitBusBranch drains every publish token
// then verifies that an over-rate publish emits the pubsub.rate_limited
// bus event (and the broker does NOT crash or remove the subscriber).
func TestBroker_HandleConn_RateLimitBusBranch(t *testing.T) {
	t.Parallel()
	bus := &stubEventBus{}
	b := newBroker(bus, nil)

	subStream, clientEnd := newPipeStreamPair()
	sub := newSubscriber(subStream)

	// Pre-drain the rate bucket BEFORE handleConn registers it so the
	// very first publish trips the rate-limit branch deterministically.
	for i := 0; i < publishBurstBudget; i++ {
		if !b.takeToken(sub) {
			t.Fatalf("token drain failed at i=%d", i)
		}
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		b.handleConn(sub)
	}()

	if err := WriteEvent(clientEnd, &Event{Topic: "rate-topic"}); err != nil {
		t.Fatalf("subscribe write: %v", err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		b.mu.RLock()
		n := len(b.subs["rate-topic"])
		b.mu.RUnlock()
		if n == 1 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	if err := WriteEvent(clientEnd, &Event{Topic: "rate-topic", Payload: []byte("dropme")}); err != nil {
		t.Fatalf("publish write: %v", err)
	}

	// Wait for the rate_limited bus event.
	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if bus.seen("pubsub.rate_limited") {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if !bus.seen("pubsub.rate_limited") {
		t.Fatalf("expected pubsub.rate_limited event, got %v", bus.topics)
	}
	// Should NOT have published since the rate limit dropped it.
	if bus.seen("pubsub.published") {
		t.Error("unexpected pubsub.published — rate limit should have dropped event")
	}

	_ = clientEnd.Close()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("handleConn did not return")
	}
}

// --- broker publishWith wildcard branch --------------------------------------

// TestPublishWith_WildcardFanout covers the for-loop over b.subs["*"]
// (lines 312-315 — wildcard subscriber receives non-wildcard event).
func TestPublishWith_WildcardFanout(t *testing.T) {
	t.Parallel()
	b := newBroker(nil, nil)

	topicSub := stubSubscriber()
	wildSub := stubSubscriber()
	b.addSub("specific", topicSub)
	b.addSub("*", wildSub)

	var mu sync.Mutex
	delivered := map[*subscriber]int{}
	write := func(s *subscriber, evt *Event) error {
		mu.Lock()
		delivered[s]++
		mu.Unlock()
		return nil
	}

	b.publishWith(&Event{Topic: "specific", Payload: []byte("p")}, stubSubscriber(), write)

	mu.Lock()
	defer mu.Unlock()
	if delivered[topicSub] != 1 {
		t.Errorf("topicSub delivered=%d, want 1", delivered[topicSub])
	}
	if delivered[wildSub] != 1 {
		t.Errorf("wildSub delivered=%d, want 1 (wildcard fan-out branch)", delivered[wildSub])
	}
}

// TestPublishWith_WildcardSenderSkipped covers the sender-skip path on
// the wildcard branch — if the sender is itself a "*" subscriber, it
// should not receive its own publish.
func TestPublishWith_WildcardSenderSkipped(t *testing.T) {
	t.Parallel()
	b := newBroker(nil, nil)

	wildSender := stubSubscriber()
	wildOther := stubSubscriber()
	b.addSub("*", wildSender)
	b.addSub("*", wildOther)

	var mu sync.Mutex
	delivered := map[*subscriber]int{}
	write := func(s *subscriber, evt *Event) error {
		mu.Lock()
		delivered[s]++
		mu.Unlock()
		return nil
	}

	b.publishWith(&Event{Topic: "anything", Payload: []byte("p")}, wildSender, write)

	mu.Lock()
	defer mu.Unlock()
	if delivered[wildSender] != 0 {
		t.Errorf("wildSender should NOT receive own publish, got %d", delivered[wildSender])
	}
	if delivered[wildOther] != 1 {
		t.Errorf("wildOther delivered=%d, want 1", delivered[wildOther])
	}
}

// TestPublishWith_TopicIsWildcardDoesNotDoubleDeliver: when the event
// topic literally is "*", the second wildcard loop is skipped (would
// otherwise double-deliver).
func TestPublishWith_TopicIsWildcardDoesNotDoubleDeliver(t *testing.T) {
	t.Parallel()
	b := newBroker(nil, nil)
	wildSub := stubSubscriber()
	b.addSub("*", wildSub)

	var mu sync.Mutex
	delivered := 0
	write := func(s *subscriber, evt *Event) error {
		mu.Lock()
		delivered++
		mu.Unlock()
		return nil
	}

	b.publishWith(&Event{Topic: "*", Payload: []byte("p")}, stubSubscriber(), write)

	mu.Lock()
	defer mu.Unlock()
	if delivered != 1 {
		t.Errorf("delivered=%d, want 1 (topic '*' must not double-fan-out)", delivered)
	}
}

// TestPublishWith_BusPublishedBranch covers the b.events != nil branch
// at the tail of publishWith (lines 349-353).
func TestPublishWith_BusPublishedBranch(t *testing.T) {
	t.Parallel()
	bus := &stubEventBus{}
	b := newBroker(bus, nil)
	sub := stubSubscriber()
	b.addSub("t", sub)

	b.publishWith(&Event{Topic: "t", Payload: []byte("p")}, stubSubscriber(),
		func(s *subscriber, evt *Event) error { return nil })

	if !bus.seen("pubsub.published") {
		t.Errorf("expected pubsub.published bus event, got %v", bus.topics)
	}
}

// --- service Stop ctx-cancel branch ------------------------------------------

// TestService_StopContextCancel covers the ctx.Done() branch in Stop:
// when the parent ctx of Stop is already cancelled but Service.done has
// not closed yet, Stop returns ctx.Err().
func TestService_StopContextCancel(t *testing.T) {
	t.Parallel()
	s := NewService()
	// Hand-build the minimal Service state Stop expects, so we don't
	// need to drive a real Start (which would race with our cancel).
	s.cancel = func() {}
	s.done = make(chan struct{}) // never closed by anyone

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	err := s.Stop(ctx)
	if err == nil {
		t.Fatal("expected non-nil error when ctx is cancelled before done closes")
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("err = %v, want context.Canceled", err)
	}
}

// --- takeToken cap branch ----------------------------------------------------

// TestBroker_AddSub_RejectsAtCap verifies that addSub returns false when
// the per-topic subscriber cap is reached (memory-DoS protection).
func TestBroker_AddSub_RejectsAtCap(t *testing.T) {
	t.Parallel()
	b := newBroker(nil, nil)
	// Seed the topic with exactly maxSubsPerTopic subscribers.
	for i := 0; i < maxSubsPerTopic; i++ {
		sub := stubSubscriber()
		if !b.addSub("full-topic", sub) {
			t.Fatalf("addSub #%d failed before cap", i)
		}
	}
	// The next subscriber must be rejected.
	extra := stubSubscriber()
	if b.addSub("full-topic", extra) {
		t.Fatal("addSub should have rejected subscriber over cap")
	}
	// A different topic should still accept subscribers (no cross-topic bleed).
	other := stubSubscriber()
	if !b.addSub("other-topic", other) {
		t.Fatal("addSub rejected subscriber on a topic well below cap")
	}
	// Verify counts.
	b.mu.RLock()
	nFull := len(b.subs["full-topic"])
	nOther := len(b.subs["other-topic"])
	b.mu.RUnlock()
	if nFull != maxSubsPerTopic {
		t.Errorf("full-topic subscribers = %d, want %d", nFull, maxSubsPerTopic)
	}
	if nOther != 1 {
		t.Errorf("other-topic subscribers = %d, want 1", nOther)
	}
}

// TestTakeToken_RefillCappedAtBurst exercises the refill cap (line
// 179-180): after a long sleep the refill would exceed the burst budget,
// so it must be clamped.
func TestTakeToken_RefillCappedAtBurst(t *testing.T) {
	t.Parallel()
	b := newBroker(nil, nil)
	sub := stubSubscriber()

	// Seed a bucket with a lastRefill far in the past so the elapsed
	// refill would massively exceed publishBurstBudget if uncapped.
	b.rateMu.Lock()
	if b.rate == nil {
		b.rate = make(map[*subscriber]*publishBucket)
	}
	b.rate[sub] = &publishBucket{
		tokens:     0,
		lastRefill: time.Now().Add(-1 * time.Hour),
	}
	b.rateMu.Unlock()

	if !b.takeToken(sub) {
		t.Fatal("first take should succeed after refill")
	}

	// Bucket should now be capped at publishBurstBudget - 1.
	b.rateMu.Lock()
	got := b.rate[sub].tokens
	b.rateMu.Unlock()

	// Allow a small slack: float64 ops produce non-integer-exact values.
	if got > float64(publishBurstBudget) {
		t.Errorf("bucket.tokens = %v, want <= %d (refill must be capped)", got, publishBurstBudget)
	}
}

// --- Server (server.go) handleConn coverage via net.Pipe --------------------

// TestServer_HandleConn_SubscribeAndPublish drives the
// (non-service) Server.handleConn end-to-end with net.Pipe connections.
// This is the biggest single coverage win for server.go: handleConn was
// previously 0%.
func TestServer_HandleConn_SubscribeAndPublish(t *testing.T) {
	t.Parallel()
	s := &Server{subs: map[string][]net.Conn{}}

	// Subscriber leg.
	subBroker, subClient := net.Pipe()
	// Publisher leg.
	pubBroker, pubClient := net.Pipe()

	subDone := make(chan struct{})
	go func() {
		defer close(subDone)
		s.handleConn(subBroker)
	}()
	pubDone := make(chan struct{})
	go func() {
		defer close(pubDone)
		s.handleConn(pubBroker)
	}()

	// Subscriber sends subscribe (topic "evt").
	if err := WriteEvent(subClient, &Event{Topic: "evt"}); err != nil {
		t.Fatalf("subscribe write: %v", err)
	}
	// Wait until both subs are registered.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		s.mu.RLock()
		n := len(s.subs["evt"])
		s.mu.RUnlock()
		if n >= 1 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	// Publisher sends subscribe (handshake) on a different topic.
	if err := WriteEvent(pubClient, &Event{Topic: "pub-handshake"}); err != nil {
		t.Fatalf("pub subscribe write: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	// Publisher publishes on "evt" — subscriber should receive it.
	if err := WriteEvent(pubClient, &Event{Topic: "evt", Payload: []byte("hello")}); err != nil {
		t.Fatalf("pub publish write: %v", err)
	}

	// Subscriber reads the delivered event.
	gotCh := make(chan *Event, 1)
	errCh := make(chan error, 1)
	go func() {
		ev, err := ReadEvent(subClient)
		if err != nil {
			errCh <- err
			return
		}
		gotCh <- ev
	}()

	select {
	case got := <-gotCh:
		if got.Topic != "evt" || string(got.Payload) != "hello" {
			t.Errorf("received %+v, want {evt hello}", got)
		}
	case err := <-errCh:
		t.Fatalf("ReadEvent: %v", err)
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for delivered event")
	}

	// Close client ends — both handleConn goroutines should exit.
	_ = subClient.Close()
	_ = pubClient.Close()
	select {
	case <-subDone:
	case <-time.After(2 * time.Second):
		t.Fatal("subscriber handleConn did not exit")
	}
	select {
	case <-pubDone:
	case <-time.After(2 * time.Second):
		t.Fatal("publisher handleConn did not exit")
	}
}

// TestServer_HandleConn_SubscribeReadFails covers the early-return EOF
// path in Server.handleConn (no subscribe event ever sent).
func TestServer_HandleConn_SubscribeReadFails(t *testing.T) {
	t.Parallel()
	s := &Server{subs: map[string][]net.Conn{}}

	broker, client := net.Pipe()
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.handleConn(broker)
	}()

	// Close immediately — ReadEvent returns io.ErrClosedPipe.
	_ = client.Close()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("handleConn did not return on early close")
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	if len(s.subs) != 0 {
		t.Errorf("subs map should be empty, got %v", s.subs)
	}
}

// TestServer_Publish_WildcardSenderSkipped: wildcard subscriber that is
// also the sender must not receive its own publish.
func TestServer_Publish_WildcardSenderSkipped(t *testing.T) {
	t.Parallel()
	s := &Server{subs: map[string][]net.Conn{}}

	sender, _ := net.Pipe()
	wild, wildPeer := net.Pipe()
	defer sender.Close()
	defer wild.Close()
	defer wildPeer.Close()

	// Sender subscribed as wildcard AND publishes a wildcard event.
	s.addSub("*", sender)
	s.addSub("*", wild)

	go s.publish(&Event{Topic: "evt", Payload: []byte("p")}, sender)

	// Only wildPeer (sister of wild) should receive — sender skipped.
	got, err := ReadEvent(wildPeer)
	if err != nil {
		t.Fatalf("ReadEvent: %v", err)
	}
	if got.Topic != "evt" {
		t.Errorf("Topic = %q", got.Topic)
	}
}

// --- Trust gate (PILOT-251) ------------------------------------------------

// stubTrustChecker is a test double that reports a fixed set of trusted
// node IDs.
type stubTrustChecker struct {
	mu      sync.Mutex
	trusted map[uint32]string
}

func (stc *stubTrustChecker) IsTrusted(nodeID uint32) (string, bool) {
	stc.mu.Lock()
	defer stc.mu.Unlock()
	name, ok := stc.trusted[nodeID]
	return name, ok
}

func (stc *stubTrustChecker) setTrusted(nodeIDs ...uint32) {
	stc.mu.Lock()
	defer stc.mu.Unlock()
	if stc.trusted == nil {
		stc.trusted = make(map[uint32]string)
	}
	for _, id := range nodeIDs {
		stc.trusted[id] = "test-peer"
	}
}

// TestBroker_HandleConn_TrustGateRejectsUntrusted verifies that
// handleConn rejects a subscriber whose node ID is not trusted when
// the broker has a TrustChecker.
func TestBroker_HandleConn_TrustGateRejectsUntrusted(t *testing.T) {
	t.Parallel()

	// Broker with a trust checker that has NO trusted peers.
	tc := &stubTrustChecker{}
	b := newBroker(nil, tc)

	subStream, clientEnd := newPipeStreamPair()
	sub := newSubscriber(subStream)

	done := make(chan struct{})
	go func() {
		defer close(done)
		b.handleConn(sub)
	}()

	// Client writes subscribe event.
	if err := WriteEvent(clientEnd, &Event{Topic: "secret"}); err != nil {
		t.Fatalf("WriteEvent subscribe: %v", err)
	}

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("handleConn should have returned after rejecting untrusted subscriber")
	}

	// The subscriber must NOT have been added.
	b.mu.RLock()
	n := len(b.subs["secret"])
	b.mu.RUnlock()
	if n != 0 {
		t.Errorf("untrusted peer added to subs, got %d subscribers, want 0", n)
	}

	_ = clientEnd.Close()
}

// TestBroker_HandleConn_TrustGateAcceptsTrusted verifies that handleConn
// accepts a subscriber whose node ID matches the trusted allowlist.
func TestBroker_HandleConn_TrustGateAcceptsTrusted(t *testing.T) {
	t.Parallel()

	// Broker with a trust checker that trusts our peer (Node = 0xBEEF
	// from pipeStream stub).
	tc := &stubTrustChecker{}
	tc.setTrusted(0xBEEF)
	b := newBroker(nil, tc)

	subStream, clientEnd := newPipeStreamPair()
	sub := newSubscriber(subStream)

	done := make(chan struct{})
	go func() {
		defer close(done)
		b.handleConn(sub)
	}()

	// Client writes subscribe event.
	if err := WriteEvent(clientEnd, &Event{Topic: "public"}); err != nil {
		t.Fatalf("WriteEvent subscribe: %v", err)
	}

	// Wait for subscription to be registered.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		b.mu.RLock()
		n := len(b.subs["public"])
		b.mu.RUnlock()
		if n == 1 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	b.mu.RLock()
	n := len(b.subs["public"])
	b.mu.RUnlock()
	if n != 1 {
		t.Errorf("trusted peer NOT added to subs, got %d subscribers, want 1", n)
	}

	// Close to let handleConn exit.
	_ = clientEnd.Close()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("handleConn did not return after client close")
	}
}

// TestServer_Publish_TopicLiteralStar covers the "evt.Topic == '*'" skip
// branch in Server.publish (line 111): when topic literally is "*", the
// wildcard fan-out loop is bypassed (would otherwise double-deliver).
func TestServer_Publish_TopicLiteralStar(t *testing.T) {
	t.Parallel()
	s := &Server{subs: map[string][]net.Conn{}}

	sender, _ := net.Pipe()
	recv, recvPeer := net.Pipe()
	defer sender.Close()
	defer recv.Close()
	defer recvPeer.Close()

	// One subscriber to "*".
	s.addSub("*", recv)

	go s.publish(&Event{Topic: "*", Payload: []byte("p")}, sender)

	// Should receive exactly one copy (no double-fan-out).
	got, err := ReadEvent(recvPeer)
	if err != nil {
		t.Fatalf("ReadEvent: %v", err)
	}
	if got.Topic != "*" {
		t.Errorf("Topic = %q", got.Topic)
	}

	// Second read should block — verify with a short deadline.
	type result struct {
		ev  *Event
		err error
	}
	done := make(chan result, 1)
	go func() {
		ev, err := ReadEvent(recvPeer)
		done <- result{ev, err}
	}()

	select {
	case r := <-done:
		t.Errorf("unexpected second delivery: %+v err=%v", r.ev, r.err)
	case <-time.After(150 * time.Millisecond):
		// expected — no second delivery.
	}
}
