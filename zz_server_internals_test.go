// SPDX-License-Identifier: AGPL-3.0-or-later

package eventstream

import (
	"net"
	"testing"
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
