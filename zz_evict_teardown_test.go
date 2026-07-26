// SPDX-License-Identifier: AGPL-3.0-or-later

//go:build !no_eventstream
// +build !no_eventstream

package eventstream

import (
	"testing"
	"time"
)

func (b *broker) rateEntries() int {
	b.rateMu.Lock()
	defer b.rateMu.Unlock()
	return len(b.rate)
}

func (b *broker) subCount() int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	n := 0
	for _, subs := range b.subs {
		n += len(subs)
	}
	return n
}

// A subscriber dropped for repeated publish failures is, by definition,
// a peer that isn't draining. Its handleConn goroutine is parked in
// ReadEvent and will stay parked — along with the stream it holds —
// unless eviction closes the stream. Deregistering alone leaks both.
func TestEvictedSubscriberIsTornDown(t *testing.T) {
	b := newBroker(nil, defaultAllowPolicy{})

	subStream, peerStream := newPipeStreamPair()
	sub := newSubscriber(subStream)

	handled := make(chan struct{})
	go func() {
		defer close(handled)
		b.handleConn(sub)
	}()

	// The peer subscribes, then goes quiet: handleConn parks on its next
	// ReadEvent, exactly like a peer that has stopped talking.
	if err := WriteEvent(peerStream, &Event{Topic: "topic-x", Payload: []byte("sub")}); err != nil {
		t.Fatalf("subscribe write: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for b.subCount() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("subscriber never registered")
		}
		time.Sleep(time.Millisecond)
	}

	// Give the subscriber a rate bucket, as any publisher would have.
	if !b.takeToken(sub) {
		t.Fatal("takeToken denied on a fresh bucket")
	}
	if b.rateEntries() != 1 {
		t.Fatalf("rate entries = %d, want 1", b.rateEntries())
	}

	// Fail past the tolerance so the subscriber is dropped.
	write, _ := alwaysFailWriter()
	for i := 0; i < maxConsecutivePublishFailures; i++ {
		b.publishWith(&Event{Topic: "topic-x", Payload: []byte("p")}, stubSubscriber(), write)
	}

	if b.subCount() != 0 {
		t.Fatalf("subscriber count = %d after eviction, want 0", b.subCount())
	}

	select {
	case <-handled:
	case <-time.After(3 * time.Second):
		t.Fatal("handleConn still parked after eviction: the stream was never closed")
	}

	if got := b.rateEntries(); got != 0 {
		t.Fatalf("rate entries = %d after eviction, want 0", got)
	}

	// The stream is closed, so the peer side sees the connection go away
	// rather than hanging on a descriptor nobody will ever read.
	_ = peerStream.Close()
	if err := WriteEvent(subStream, &Event{Topic: "topic-x"}); err == nil {
		t.Fatal("evicted subscriber's stream is still open")
	}
}
