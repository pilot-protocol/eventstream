// SPDX-License-Identifier: AGPL-3.0-or-later

package eventstream

import (
	"github.com/pilot-protocol/common/decision"
	"github.com/pilot-protocol/common/driver"
	"github.com/pilot-protocol/common/protocol"
)

// Client connects to a remote event stream broker on port 1002.
type Client struct {
	conn  *driver.Conn
	topic string
}

// Subscribe connects to the event stream and subscribes to a topic.
// Use "*" to subscribe to all events.
func Subscribe(d *driver.Driver, addr protocol.Addr, topic string) (*Client, error) {
	conn, err := d.DialAddr(addr, protocol.PortEventStream)
	if err != nil {
		return nil, err
	}

	// Send subscription event
	if err := WriteEvent(conn, &Event{Topic: topic}); err != nil {
		conn.Close()
		return nil, err
	}

	return &Client{conn: conn, topic: topic}, nil
}

// Publish sends an event to the broker for distribution.
func (c *Client) Publish(topic string, payload []byte) error {
	return WriteEvent(c.conn, &Event{Topic: topic, Payload: payload})
}

// PublishGoverned publishes an event with its exact signed intent and
// authority decision. A broker configured to require governed publications
// verifies the envelope before forwarding the inner event to subscribers.
func (c *Client) PublishGoverned(event *Event, intent decision.Intent, result decision.Decision) error {
	governed, err := NewGovernedEvent(event, intent, result)
	if err != nil {
		return err
	}
	envelope, err := EncodeGovernedEvent(governed)
	if err != nil {
		return err
	}
	return WriteEvent(c.conn, envelope)
}

// PublishGovernedWithDisclosure publishes a signed event whose Intent binds
// typed disclosure metadata. The broker can require this form per topic.
func (c *Client) PublishGovernedWithDisclosure(event *Event, intent decision.Intent, result decision.Decision, disclosure decision.DisclosureBinding) error {
	governed, err := NewGovernedEventWithDisclosure(event, intent, result, disclosure)
	if err != nil {
		return err
	}
	envelope, err := EncodeGovernedEvent(governed)
	if err != nil {
		return err
	}
	return WriteEvent(c.conn, envelope)
}

// Recv waits for the next event from the broker.
func (c *Client) Recv() (*Event, error) {
	return ReadEvent(c.conn)
}

// Close closes the connection.
func (c *Client) Close() error {
	return c.conn.Close()
}
