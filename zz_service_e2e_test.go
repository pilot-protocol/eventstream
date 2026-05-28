// SPDX-License-Identifier: AGPL-3.0-or-later

//go:build !no_eventstream
// +build !no_eventstream

package eventstream

import (
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/pilot-protocol/common/coreapi"
	"github.com/pilot-protocol/common/protocol"
)

type esFakeStream struct {
	r         *io.PipeReader
	w         *io.PipeWriter
	closeOnce sync.Once
}

func (s *esFakeStream) Read(p []byte) (int, error)   { return s.r.Read(p) }
func (s *esFakeStream) Write(p []byte) (int, error)  { return s.w.Write(p) }
func (s *esFakeStream) Close() error {
	s.closeOnce.Do(func() {
		_ = s.r.Close()
		_ = s.w.Close()
	})
	return nil
}
func (s *esFakeStream) LocalAddr() coreapi.Addr            { return protocol.Addr{} }
func (s *esFakeStream) LocalPort() uint16                  { return 1002 }
func (s *esFakeStream) RemoteAddr() coreapi.Addr           { return protocol.Addr{Node: 0xCAFE} }
func (s *esFakeStream) RemotePort() uint16                 { return 55000 }
func (s *esFakeStream) SetDeadline(time.Time) error        { return nil }
func (s *esFakeStream) SetReadDeadline(time.Time) error    { return nil }
func (s *esFakeStream) SetWriteDeadline(time.Time) error   { return nil }

type esFakeListener struct {
	streams   []coreapi.Stream
	idx       int
	mu        sync.Mutex
	closeCh   chan struct{}
	closeOnce sync.Once
}

func newESListener(streams ...coreapi.Stream) *esFakeListener {
	return &esFakeListener{streams: streams, closeCh: make(chan struct{})}
}

func (l *esFakeListener) Accept() (coreapi.Stream, error) {
	l.mu.Lock()
	if l.idx < len(l.streams) {
		s := l.streams[l.idx]
		l.idx++
		l.mu.Unlock()
		return s, nil
	}
	l.mu.Unlock()
	<-l.closeCh
	return nil, errors.New("listener closed")
}

func (l *esFakeListener) Close() error {
	l.closeOnce.Do(func() { close(l.closeCh) })
	return nil
}

func (l *esFakeListener) Addr() coreapi.Addr { return protocol.Addr{} }
func (l *esFakeListener) Port() uint16       { return 1002 }

type esFakeStreams struct {
	ln       coreapi.Listener
	listenErr error
}

func (s *esFakeStreams) Listen(uint16) (coreapi.Listener, error) {
	if s.listenErr != nil {
		return nil, s.listenErr
	}
	return s.ln, nil
}
func (s *esFakeStreams) Dial(context.Context, coreapi.Addr, uint16) (coreapi.Stream, error) {
	return nil, errors.New("Dial stub")
}
func (s *esFakeStreams) SendDatagram(context.Context, coreapi.Addr, uint16, []byte) error {
	return errors.New("SendDatagram stub")
}

func TestService_StartListenError(t *testing.T) {
	t.Parallel()
	s := NewService()
	deps := coreapi.Deps{Streams: &esFakeStreams{listenErr: errors.New("listen failed")}}
	if err := s.Start(context.Background(), deps); err == nil {
		t.Error("expected Start to propagate listen error")
	}
}

// TestService_AcceptLoopAndPubSub drives the full broker:
// subscriber connects → reads its first Event (subscribe), then a second
// goroutine publishes and the subscriber receives the published event.
func TestService_AcceptLoopAndPubSub(t *testing.T) {
	t.Parallel()
	// Build subscriber & publisher pairs.
	subC2sR, subC2sW := io.Pipe()
	subS2cR, subS2cW := io.Pipe()
	subStream := &esFakeStream{r: subC2sR, w: subS2cW}

	pubC2sR, pubC2sW := io.Pipe()
	// Use a simpler shape: discard the publisher's reads.
	pubStream := newSinkStream(pubC2sR)

	listener := newESListener(subStream, pubStream)
	deps := coreapi.Deps{Streams: &esFakeStreams{ln: listener}}

	s := NewService()
	if err := s.Start(context.Background(), deps); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = s.Stop(context.Background()) })

	// 1) Subscriber: write a "subscribe" Event with topic "shared".
	if err := WriteEvent(subC2sW, &Event{Topic: "shared", Payload: nil}); err != nil {
		t.Fatalf("WriteEvent subscribe: %v", err)
	}

	// Give the broker time to register the subscription.
	time.Sleep(100 * time.Millisecond)

	// 2) Publisher: first event is the subscribe-handshake (to some
	// topic the test doesn't care about); the second is the publish.
	if err := WriteEvent(pubC2sW, &Event{Topic: "publisher", Payload: nil}); err != nil {
		t.Fatalf("WriteEvent pub subscribe: %v", err)
	}
	// Allow the publisher's subscribe to land.
	time.Sleep(50 * time.Millisecond)
	if err := WriteEvent(pubC2sW, &Event{Topic: "shared", Payload: []byte("hello")}); err != nil {
		t.Fatalf("WriteEvent publish: %v", err)
	}

	// 3) Subscriber should receive the published event.
	gotCh := make(chan *Event, 1)
	errCh := make(chan error, 1)
	go func() {
		got, err := ReadEvent(subS2cR)
		if err != nil {
			errCh <- err
			return
		}
		gotCh <- got
	}()

	select {
	case got := <-gotCh:
		if got.Topic != "shared" {
			t.Errorf("Topic = %q, want shared", got.Topic)
		}
		if string(got.Payload) != "hello" {
			t.Errorf("Payload = %q, want hello", got.Payload)
		}
	case err := <-errCh:
		t.Fatalf("ReadEvent: %v", err)
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for delivered event")
	}

	// Tear down to drain goroutines.
	_ = subC2sW.Close()
	_ = subS2cR.Close()
	_ = pubC2sW.Close()
}

// newSinkStream returns an esFakeStream wired with a real PipeReader
// for the inbound side and a draining PipeWriter for the outbound side.
func newSinkStream(r *io.PipeReader) *esFakeStream {
	// Use an os.Pipe-style discard writer.
	dw := &esFakeStream{}
	dw.r = r
	pr, pw := io.Pipe()
	dw.w = pw
	// Drain pw so writes don't block.
	go func() {
		buf := make([]byte, 1024)
		for {
			if _, err := pr.Read(buf); err != nil {
				return
			}
		}
	}()
	return dw
}

func TestService_StopWithoutStart(t *testing.T) {
	t.Parallel()
	s := NewService()
	if err := s.Stop(context.Background()); err != nil {
		t.Errorf("Stop without Start: %v", err)
	}
}
