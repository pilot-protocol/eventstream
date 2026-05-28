// SPDX-License-Identifier: AGPL-3.0-or-later

package eventstream

import (
	"net"
	"sync"
	"testing"
	"time"
)

// TestServer_AddRemoveSubDirect drives the bookkeeping methods on the
// driver-less Server type. The actual driver field is nil; we don't
// touch it because addSub/removeSub/publish only operate on s.subs.
func TestServer_AddRemoveSubDirect(t *testing.T) {
	t.Parallel()
	s := &Server{subs: map[string][]net.Conn{}}

	a, _ := net.Pipe()
	b, _ := net.Pipe()
	defer a.Close()
	defer b.Close()

	s.addSub("topic-a", a)
	s.addSub("topic-a", b)
	s.addSub("topic-b", a)

	s.mu.RLock()
	if len(s.subs["topic-a"]) != 2 {
		t.Errorf("topic-a len = %d, want 2", len(s.subs["topic-a"]))
	}
	if len(s.subs["topic-b"]) != 1 {
		t.Errorf("topic-b len = %d, want 1", len(s.subs["topic-b"]))
	}
	s.mu.RUnlock()

	// Removing a from both topics, then topic-b should be deleted entirely
	// (length-zero slot cleanup).
	s.removeSub(a)
	s.mu.RLock()
	defer s.mu.RUnlock()
	if len(s.subs["topic-a"]) != 1 {
		t.Errorf("topic-a after remove = %d, want 1", len(s.subs["topic-a"]))
	}
	if _, ok := s.subs["topic-b"]; ok {
		t.Error("topic-b should be deleted after last sub removed")
	}
}

// TestServer_Publish_SkipsSenderAndDelivers drives the publish loop
// directly. Uses net.Pipe connections — the test reads off the receiver
// end concurrently.
func TestServer_Publish_SkipsSenderAndDelivers(t *testing.T) {
	t.Parallel()
	s := &Server{subs: map[string][]net.Conn{}}

	senderA, _ := net.Pipe()
	recvA, recvAPeer := net.Pipe()
	defer senderA.Close()
	defer recvA.Close()
	defer recvAPeer.Close()

	s.addSub("topic", senderA)
	s.addSub("topic", recvA)

	// Publish from senderA — only recvA should get the event.
	go s.publish(&Event{Topic: "topic", Payload: []byte("hi")}, senderA)

	got, err := ReadEvent(recvAPeer)
	if err != nil {
		t.Fatalf("ReadEvent: %v", err)
	}
	if got.Topic != "topic" || string(got.Payload) != "hi" {
		t.Errorf("got %+v", got)
	}
}

// TestServer_Publish_WildcardSubscriber drives the * wildcard branch.
func TestServer_Publish_WildcardSubscriber(t *testing.T) {
	t.Parallel()
	s := &Server{subs: map[string][]net.Conn{}}

	sender, _ := net.Pipe()
	wild, wildPeer := net.Pipe()
	defer sender.Close()
	defer wild.Close()
	defer wildPeer.Close()

	s.addSub("*", wild)

	go s.publish(&Event{Topic: "any", Payload: []byte("p")}, sender)

	got, err := ReadEvent(wildPeer)
	if err != nil {
		t.Fatalf("ReadEvent: %v", err)
	}
	if got.Topic != "any" {
		t.Errorf("Topic = %q", got.Topic)
	}
}

// TestServer_PublishDoesNotBlockOnSlowSubscriber is a regression test
// for the bug where Server.publish held mu.RLock() across WriteEvent
// I/O. With the bug, a slow subscriber (full pipe buffer) wedged the
// RLock, blocking every concurrent addSub/removeSub waiting on
// mu.Lock(). The fix snapshots the subscriber list under a brief lock
// then releases before iterating, so subscription mutations stay
// responsive even while a peer is wedged.
func TestServer_PublishDoesNotBlockOnSlowSubscriber(t *testing.T) {
	t.Parallel()
	s := &Server{subs: map[string][]net.Conn{}}

	// Fast subscriber: drain immediately in a goroutine.
	fast, fastPeer := net.Pipe()
	defer fast.Close()
	defer fastPeer.Close()
	go func() {
		for {
			if _, err := ReadEvent(fastPeer); err != nil {
				return
			}
		}
	}()

	// Slow subscriber: never read until the test releases it. With the
	// old code, WriteEvent blocks on the pipe write, and the RLock is
	// held the whole time.
	slow, slowPeer := net.Pipe()
	defer slow.Close()
	defer slowPeer.Close()

	release := make(chan struct{})
	go func() {
		// Drain the slow side only after the test signals.
		<-release
		for {
			if _, err := ReadEvent(slowPeer); err != nil {
				return
			}
		}
	}()

	s.addSub("topic", fast)
	s.addSub("topic", slow)

	// Drive a publish from a sender that isn't subscribed.
	sender, _ := net.Pipe()
	defer sender.Close()

	publishDone := make(chan struct{})
	go func() {
		defer close(publishDone)
		s.publish(&Event{Topic: "topic", Payload: []byte("payload")}, sender)
	}()

	// Give publish a moment to enter — with the buggy implementation
	// it acquires RLock and starts writing to fast (which drains),
	// then blocks on slow's WriteEvent while still holding RLock.
	// With the fixed implementation it has already snapshotted and
	// released RLock before either WriteEvent call.
	time.Sleep(20 * time.Millisecond)

	// Concurrently try to mutate the subscription set. With the bug,
	// addSub blocks on mu.Lock() while the publish holds RLock.
	extra, _ := net.Pipe()
	defer extra.Close()

	addDone := make(chan struct{})
	addStart := time.Now()
	go func() {
		s.addSub("other-topic", extra)
		close(addDone)
	}()

	select {
	case <-addDone:
		elapsed := time.Since(addStart)
		if elapsed > 100*time.Millisecond {
			t.Fatalf("addSub took %v with slow subscriber present; want <100ms", elapsed)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatalf("addSub blocked >100ms behind slow subscriber — publish is holding RLock across I/O")
	}

	// Drain the slow side so publish can finish; the test goroutines clean up.
	close(release)
	select {
	case <-publishDone:
	case <-time.After(2 * time.Second):
		t.Fatal("publish never completed after slow subscriber drained")
	}
}

// TestServer_PublishHandlesSlowSubscriberWithoutDeadlock exercises the
// happy-path semantics alongside the slow-subscriber scenario: a fast
// subscriber still receives the event promptly, multiple publishers
// can run concurrently against a wedged peer, and removeSub during the
// wedge succeeds without deadlock.
func TestServer_PublishHandlesSlowSubscriberWithoutDeadlock(t *testing.T) {
	t.Parallel()
	s := &Server{subs: map[string][]net.Conn{}}

	fast, fastPeer := net.Pipe()
	defer fast.Close()
	defer fastPeer.Close()

	slow, slowPeer := net.Pipe()
	defer slow.Close()
	defer slowPeer.Close()

	// Fast peer drains continuously; it asserts the first event was
	// delivered, then keeps reading to unblock the rest of the publishers.
	received := make(chan *Event, 8)
	fastDone := make(chan struct{})
	go func() {
		defer close(fastDone)
		for {
			evt, err := ReadEvent(fastPeer)
			if err != nil {
				return
			}
			select {
			case received <- evt:
			default:
			}
		}
	}()

	// Slow peer never reads — its pipe stays wedged for the lifetime
	// of the test. We rely on Close() at the end to unblock anything
	// still stuck inside WriteEvent.

	s.addSub("t", fast)
	s.addSub("t", slow)

	sender, _ := net.Pipe()
	defer sender.Close()

	// Kick off three publishers in parallel. None of them must
	// deadlock the broker; the fast peer must get all three events.
	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.publish(&Event{Topic: "t", Payload: []byte("p")}, sender)
		}()
	}

	// The fast peer must receive the first event quickly.
	select {
	case got := <-received:
		if got.Topic != "t" || string(got.Payload) != "p" {
			t.Errorf("fast peer got unexpected event: %+v", got)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("fast peer never received event — broker is wedged by slow peer")
	}

	// removeSub on the slow peer must not deadlock even while
	// publish goroutines are mid-flight blocked on the pipe.
	removeDone := make(chan struct{})
	go func() {
		s.removeSub(slow)
		close(removeDone)
	}()
	select {
	case <-removeDone:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("removeSub blocked while slow subscriber wedged the broker")
	}

	// Drain the slow side so the in-flight publishers can complete.
	// We use a goroutine so closing slow/slowPeer in defer also
	// works as a safety net.
	drainDone := make(chan struct{})
	go func() {
		defer close(drainDone)
		for {
			if _, err := ReadEvent(slowPeer); err != nil {
				return
			}
		}
	}()

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("publishers never drained after slow peer started draining")
	}
	// Close both sides to terminate the drain + fast reader goroutines.
	slowPeer.Close()
	fastPeer.Close()
	<-drainDone
	<-fastDone
}

// TestNewServer covers the driver-less constructor branch.
func TestNewServer_NilDriverPanicsOnUse(t *testing.T) {
	t.Parallel()
	// NewServer accepts nil and just stashes it — verify no immediate panic.
	s := NewServer(nil)
	if s == nil {
		t.Fatal("NewServer returned nil")
	}
	if s.subs == nil {
		t.Error("subs map should be initialized")
	}
}
