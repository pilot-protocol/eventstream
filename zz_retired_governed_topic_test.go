// SPDX-License-Identifier: AGPL-3.0-or-later

//go:build !no_eventstream
// +build !no_eventstream

package eventstream

import (
	"testing"
	"time"
)

// The governed publication envelope is gone, but its transport topic stays
// reserved: a peer that still sends one (an older node, or anyone crafting
// the frame) must be refused, and the envelope must never reach a subscriber
// — not one on "*", and not one subscribed to the reserved name itself. The
// publisher's connection survives and its next ordinary event is delivered.
func TestBroker_RetiredGovernedTopicIsRefusedNotFannedOut(t *testing.T) {
	t.Parallel()
	if retiredGovernedTopic != "\x00pilot.governed.v1" {
		t.Fatalf("reserved topic changed on the wire: %q", retiredGovernedTopic)
	}
	bus := &stubEventBus{}
	b := newBroker(bus, defaultAllowPolicy{})

	connect := func(topic string) (*pipeStream, chan struct{}) {
		t.Helper()
		server, client := newPipeStreamPair()
		done := make(chan struct{})
		go func() {
			defer close(done)
			b.handleConn(newSubscriber(server))
		}()
		if err := WriteEvent(client, &Event{Topic: topic}); err != nil {
			t.Fatalf("subscribe %q: %v", topic, err)
		}
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			b.mu.RLock()
			n := len(b.subs[topic])
			b.mu.RUnlock()
			if n == 1 {
				return client, done
			}
			time.Sleep(5 * time.Millisecond)
		}
		t.Fatalf("subscription to %q was not registered", topic)
		return nil, nil
	}
	receive := func(client *pipeStream) chan *Event {
		got := make(chan *Event, 4)
		go func() {
			for {
				evt, err := ReadEvent(client)
				if err != nil {
					return
				}
				got <- evt
			}
		}()
		return got
	}

	wildcard, wildcardDone := connect("*")
	reserved, reservedDone := connect(retiredGovernedTopic)
	publisher, publisherDone := connect("publisher")
	wildcardGot := receive(wildcard)
	reservedGot := receive(reserved)

	envelope := []byte(`{"version":1,"topic":"alerts","payload":"c2VjcmV0"}`)
	if err := WriteEvent(publisher, &Event{Topic: retiredGovernedTopic, Payload: envelope}); err != nil {
		t.Fatalf("write reserved-topic publication: %v", err)
	}
	if err := WriteEvent(publisher, &Event{Topic: "alerts", Payload: []byte("ordinary")}); err != nil {
		t.Fatalf("write ordinary publication: %v", err)
	}

	// The broker handles a publisher's events in order, so the first thing
	// the wildcard subscriber sees must be the ordinary event.
	select {
	case evt := <-wildcardGot:
		if evt.Topic != "alerts" || string(evt.Payload) != "ordinary" {
			t.Fatalf("wildcard subscriber received %q %q, want the ordinary event only", evt.Topic, evt.Payload)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("ordinary publication after a refused one was not delivered")
	}
	select {
	case evt := <-wildcardGot:
		t.Fatalf("wildcard subscriber received a second event: %q", evt.Topic)
	case evt := <-reservedGot:
		t.Fatalf("subscriber to the reserved topic received %q", evt.Topic)
	case <-time.After(150 * time.Millisecond):
	}
	if n := bus.count("pubsub.publish_denied"); n != 1 {
		t.Fatalf("pubsub.publish_denied events = %d, want 1 (%v)", n, bus.topics)
	}
	if n := bus.count("pubsub.published"); n != 1 {
		t.Fatalf("pubsub.published events = %d, want 1 (%v)", n, bus.topics)
	}

	for _, c := range []*pipeStream{publisher, wildcard, reserved} {
		_ = c.Close()
	}
	for _, done := range []chan struct{}{publisherDone, wildcardDone, reservedDone} {
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("handleConn did not return after its client closed")
		}
	}
}
