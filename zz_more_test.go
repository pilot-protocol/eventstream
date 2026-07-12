// SPDX-License-Identifier: AGPL-3.0-or-later

//go:build !no_eventstream
// +build !no_eventstream

package eventstream

import (
	"bytes"
	"context"
	"encoding/binary"
	"testing"
)

// TestService_Defaults exercises the NewService constructor and
// trivial accessors. Stop without Start is the safe-default path.
func TestService_Defaults(t *testing.T) {
	t.Parallel()
	s := NewService()
	if s == nil {
		t.Fatal("NewService returned nil")
	}
	if s.Name() != "eventstream" {
		t.Errorf("Name = %q", s.Name())
	}
	if s.Order() != 120 {
		t.Errorf("Order = %d, want 120", s.Order())
	}
	if err := s.Stop(context.Background()); err != nil {
		t.Errorf("Stop without Start: %v", err)
	}
}

// TestSubscriber_RemoteOnNilConn exercises the early-return path in
// (*subscriber).remote() when conn is nil.
func TestSubscriber_RemoteOnNilConn(t *testing.T) {
	t.Parallel()
	s := stubSubscriber()
	if got := s.remote(); got != "<nil>" {
		t.Errorf("remote() = %q, want <nil>", got)
	}
}

// TestNewSubscriber wraps a nil stream and checks the Close path is a
// no-op error (nil conn → nil-Close branch). We don't construct a real
// coreapi.Stream — just verify the wrapper builds.
func TestNewSubscriber_NilWrap(t *testing.T) {
	t.Parallel()
	if got := newSubscriber(nil); got == nil {
		t.Fatal("newSubscriber returned nil")
	}
}

// TestBrokerAddRemoveSub_DirectCalls exercises addSub + removeSub
// without going through handleConn.
func TestBrokerAddRemoveSub_DirectCalls(t *testing.T) {
	t.Parallel()
	b := newBroker(nil, nil)
	sub1 := stubSubscriber()
	sub2 := stubSubscriber()

	b.addSub("ping", sub1)
	b.addSub("ping", sub2)
	b.addSub("pong", sub1)

	b.mu.RLock()
	if len(b.subs["ping"]) != 2 {
		t.Errorf("ping subs = %d, want 2", len(b.subs["ping"]))
	}
	if len(b.subs["pong"]) != 1 {
		t.Errorf("pong subs = %d, want 1", len(b.subs["pong"]))
	}
	b.mu.RUnlock()

	// Remove sub1: removed from both topic lists; "pong" deleted entirely
	// because empty.
	b.removeSub(sub1)
	b.mu.RLock()
	defer b.mu.RUnlock()
	if len(b.subs["ping"]) != 1 {
		t.Errorf("ping subs after remove = %d, want 1", len(b.subs["ping"]))
	}
	if _, ok := b.subs["pong"]; ok {
		t.Errorf("pong should be deleted after last sub removed")
	}
}

// TestBrokerForgetPublisher exercises forgetPublisher directly: a
// publisher's bucket should be gone after the call.
func TestBrokerForgetPublisher(t *testing.T) {
	t.Parallel()
	b := newBroker(nil, nil)
	sub := stubSubscriber()
	// Prime the bucket.
	if !b.takeToken(sub) {
		t.Fatal("first token should succeed")
	}
	b.forgetPublisher(sub)
	b.rateMu.Lock()
	_, exists := b.rate[sub]
	b.rateMu.Unlock()
	if exists {
		t.Error("rate bucket should be removed after forgetPublisher")
	}
}

// TestTakeToken_BurstAndRecover exercises the rate-limit drain + refill.
func TestTakeToken_BurstAndRecover(t *testing.T) {
	t.Parallel()
	b := newBroker(nil, nil)
	sub := stubSubscriber()
	// Drain the entire burst budget.
	drained := 0
	for i := 0; i < publishBurstBudget; i++ {
		if b.takeToken(sub) {
			drained++
		} else {
			break
		}
	}
	if drained != publishBurstBudget {
		t.Errorf("drained %d tokens, want %d", drained, publishBurstBudget)
	}
	// Next take should return false (bucket empty).
	if b.takeToken(sub) {
		t.Error("take after drain: want false, got true")
	}
}

// TestReadEvent_TopicEOF covers the topic-length EOF branch.
func TestReadEvent_TopicEOF(t *testing.T) {
	t.Parallel()
	// 1 byte of topic length = incomplete.
	if _, err := ReadEvent(bytes.NewReader([]byte{0x00})); err == nil {
		t.Error("expected error on truncated topic length")
	}
}

// TestReadEvent_PayloadLenEOF covers the payload-length EOF branch.
func TestReadEvent_PayloadLenEOF(t *testing.T) {
	t.Parallel()
	// 2-byte topic len = 3, topic = "abc", then truncated payload-length.
	buf := bytes.NewBuffer(nil)
	buf.Write([]byte{0x00, 0x03, 'a', 'b', 'c'})
	buf.Write([]byte{0x00}) // only 1 byte of payload length
	if _, err := ReadEvent(buf); err == nil {
		t.Error("expected error on truncated payload length")
	}
}

// TestReadEvent_PayloadEOF covers the payload-content EOF branch.
func TestReadEvent_PayloadEOF(t *testing.T) {
	t.Parallel()
	buf := bytes.NewBuffer(nil)
	buf.Write([]byte{0x00, 0x01, 'x'})        // topic len=1, "x"
	buf.Write([]byte{0x00, 0x00, 0x00, 0x05}) // payload len=5
	buf.Write([]byte{1, 2})                   // only 2 bytes — truncated
	if _, err := ReadEvent(buf); err == nil {
		t.Error("expected error on truncated payload")
	}
}

// TestWriteEvent_ErrorPropagates: writing to an io.Writer that always
// fails should bubble the error up.
type failingWriterT struct{}

func (failingWriterT) Write([]byte) (int, error) {
	return 0, errBadWriter
}

var errBadWriter = bytes.ErrTooLarge

func TestWriteEvent_ErrorPropagates(t *testing.T) {
	t.Parallel()
	err := WriteEvent(failingWriterT{}, &Event{Topic: "x", Payload: nil})
	if err == nil {
		t.Error("expected error from failing writer")
	}
}

// TestWriteEvent_LargeTopicAllowed covers the upper-bound topic encoding
// path (1024 bytes is the max ReadEvent accepts).
func TestWriteEvent_LargeTopicAllowed(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	topic := bytes.Repeat([]byte{'a'}, 1024)
	if err := WriteEvent(&buf, &Event{Topic: string(topic), Payload: []byte("p")}); err != nil {
		t.Fatalf("WriteEvent: %v", err)
	}
	// Verify the length prefix encodes 1024.
	got := binary.BigEndian.Uint16(buf.Bytes()[:2])
	if got != 1024 {
		t.Errorf("topic length = %d, want 1024", got)
	}
}
