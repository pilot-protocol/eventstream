// SPDX-License-Identifier: AGPL-3.0-or-later

package eventstream

import (
	"log/slog"
	"net"
	"sync"

	"github.com/pilot-protocol/common/driver"
	"github.com/pilot-protocol/common/protocol"
)

// Server is a pub/sub event broker on port 1002.
// Clients connect, subscribe to topics, and publish events.
// The first event from a client is treated as a subscription:
// - Topic "*" subscribes to all events
// - Any other topic subscribes to that specific topic
// Subsequent events are published to all matching subscribers.
type Server struct {
	driver *driver.Driver
	mu     sync.RWMutex
	subs   map[string][]net.Conn // topic → subscribers
}

// NewServer creates an event stream server.
func NewServer(d *driver.Driver) *Server {
	return &Server{
		driver: d,
		subs:   make(map[string][]net.Conn),
	}
}

// ListenAndServe binds port 1002 and starts the broker.
func (s *Server) ListenAndServe() error {
	ln, err := s.driver.Listen(protocol.PortEventStream)
	if err != nil {
		return err
	}

	slog.Info("eventstream listening", "port", protocol.PortEventStream)

	for {
		conn, err := ln.Accept()
		if err != nil {
			return err
		}
		go s.handleConn(conn)
	}
}

func (s *Server) handleConn(conn net.Conn) {
	defer func() {
		s.removeSub(conn)
		conn.Close()
	}()

	// First event = subscription
	subEvt, err := ReadEvent(conn)
	if err != nil {
		return
	}

	topic := subEvt.Topic
	s.addSub(topic, conn)
	slog.Debug("eventstream subscription", "remote", conn.RemoteAddr(), "topic", topic)

	// Remaining events = publish
	for {
		evt, err := ReadEvent(conn)
		if err != nil {
			return
		}
		s.publish(evt, conn)
	}
}

func (s *Server) addSub(topic string, conn net.Conn) {
	s.mu.Lock()
	s.subs[topic] = append(s.subs[topic], conn)
	s.mu.Unlock()
}

func (s *Server) removeSub(conn net.Conn) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for topic, conns := range s.subs {
		for i, c := range conns {
			if c == conn {
				s.subs[topic] = append(conns[:i], conns[i+1:]...)
				break
			}
		}
		if len(s.subs[topic]) == 0 {
			delete(s.subs, topic)
		}
	}
}

// publish fan-outs evt to every subscriber on evt.Topic and "*" (minus
// the sender). The subscriber set is snapshotted under a brief RLock
// and the lock is released before any WriteEvent I/O — otherwise a slow
// subscriber (full pipe / TCP send buffer) would block WriteEvent while
// the RLock is still held, starving every concurrent addSub/removeSub
// caller waiting on mu.Lock(). See broker.publishWith in service.go for
// the same pattern (P2-003).
func (s *Server) publish(evt *Event, sender net.Conn) {
	s.mu.RLock()
	targets := make([]net.Conn, 0, len(s.subs[evt.Topic])+len(s.subs["*"]))
	for _, conn := range s.subs[evt.Topic] {
		if conn != sender {
			targets = append(targets, conn)
		}
	}
	if evt.Topic != "*" {
		for _, conn := range s.subs["*"] {
			if conn != sender {
				targets = append(targets, conn)
			}
		}
	}
	s.mu.RUnlock()

	for _, conn := range targets {
		if err := WriteEvent(conn, evt); err != nil {
			slog.Debug("eventstream write failed",
				"remote", conn.RemoteAddr(),
				"topic", evt.Topic,
				"error", err)
		}
	}

	slog.Debug("eventstream published",
		"topic", evt.Topic,
		"bytes", len(evt.Payload),
		"from", sender.RemoteAddr(),
		"targets", len(targets))
}
