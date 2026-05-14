// Package colibri implements the Jitsi Videobridge bridge channel protocol over WebSocket.
//
// This is the modern replacement for the SCTP DataChannel in Jitsi Meet. The WebSocket URL is
// delivered inside the Jingle session-initiate from Jicofo (under <transport><web-socket url=.../>).
// All messages are JSON with a "colibriClass" discriminator.
//
// References:
//   - https://github.com/jitsi/jitsi-videobridge/blob/master/jvb/src/main/kotlin/org/jitsi/videobridge/message/BridgeChannelMessage.kt
package colibri

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/coder/websocket"
)

// Conn wraps the colibri-ws connection to a JVB.
type Conn struct {
	ws       *websocket.Conn
	url      string
	mu       sync.Mutex
	incoming chan Message
	closed   chan struct{}
	closeOnce sync.Once
}

// Message is any incoming bridge channel message. RawJSON is the original JSON, Class is the
// colibriClass attribute, Fields holds parsed top-level JSON fields (so callers can read e.g.
// "from", "to", custom payloads, etc.).
type Message struct {
	Class   string
	From    string
	To      string
	Fields  map[string]any
	RawJSON []byte
}

// Dial opens a colibri-ws WebSocket and performs the optional ClientHello handshake.
func Dial(ctx context.Context, url string) (*Conn, error) {
	ws, _, err := websocket.Dial(ctx, url, nil)
	if err != nil {
		return nil, fmt.Errorf("dial: %w", err)
	}
	ws.SetReadLimit(16 << 20) // 16 MiB
	c := &Conn{
		ws:       ws,
		url:      url,
		incoming: make(chan Message, 64),
		closed:   make(chan struct{}),
	}
	// send ClientHello as required by the protocol
	if err := c.SendJSON(map[string]any{"colibriClass": "ClientHello"}); err != nil {
		ws.Close(websocket.StatusInternalError, "")
		return nil, fmt.Errorf("client hello: %w", err)
	}
	go c.readLoop()
	return c, nil
}

// URL returns the WebSocket URL used for this connection.
func (c *Conn) URL() string { return c.url }

// Messages returns the incoming message channel. It is closed when the connection is torn down.
func (c *Conn) Messages() <-chan Message { return c.incoming }

// Close closes the WebSocket.
func (c *Conn) Close() error {
	c.closeOnce.Do(func() { close(c.closed) })
	return c.ws.Close(websocket.StatusNormalClosure, "")
}

func (c *Conn) readLoop() {
	defer close(c.incoming)
	for {
		_, data, err := c.ws.Read(context.Background())
		if err != nil {
			return
		}
		var fields map[string]any
		if err := json.Unmarshal(data, &fields); err != nil {
			continue
		}
		class, _ := fields["colibriClass"].(string)
		from, _ := fields["from"].(string)
		to, _ := fields["to"].(string)
		msg := Message{
			Class:   class,
			From:    from,
			To:      to,
			Fields:  fields,
			RawJSON: append([]byte(nil), data...),
		}
		select {
		case c.incoming <- msg:
		case <-c.closed:
			return
		}
	}
}

// SendJSON serialises and sends an arbitrary JSON message. Caller is responsible for setting
// "colibriClass".
func (c *Conn) SendJSON(payload any) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	return c.sendBytes(data)
}

func (c *Conn) sendBytes(data []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.ws.Write(context.Background(), websocket.MessageText, data)
}

// SendRaw sends arbitrary opaque bytes as a single broadcast EndpointMessage. The bytes are
// base64-encoded and placed under the "raw" field; receivers should pass through DecodeRaw.
//
// to == "" means broadcast to every other endpoint in the conference.
func (c *Conn) SendRaw(to string, payload []byte) error {
	msg := map[string]any{
		"colibriClass": "EndpointMessage",
		"to":           to,
		"raw":          base64.StdEncoding.EncodeToString(payload),
	}
	return c.SendJSON(msg)
}

// DecodeRaw extracts the bytes from an EndpointMessage that was produced by SendRaw.
// Returns nil if the message is not a raw frame.
func DecodeRaw(m Message) []byte {
	if m.Class != "EndpointMessage" {
		return nil
	}
	enc, ok := m.Fields["raw"].(string)
	if !ok {
		return nil
	}
	b, err := base64.StdEncoding.DecodeString(enc)
	if err != nil {
		return nil
	}
	return b
}

// SendEndpointMessage sends a JSON-payload EndpointMessage to a specific endpoint or broadcast
// (to == ""). Any extra fields in extras are merged at the top level (alongside colibriClass/to).
//
// Example:
//
//	conn.SendEndpointMessage("", map[string]any{"text": "hello", "type": "chat"})
//
// The bridge does not interpret these fields — it simply forwards them to the receiver(s).
func (c *Conn) SendEndpointMessage(to string, extras map[string]any) error {
	msg := map[string]any{
		"colibriClass": "EndpointMessage",
		"to":           to,
	}
	for k, v := range extras {
		if k == "colibriClass" || k == "to" {
			continue
		}
		msg[k] = v
	}
	return c.SendJSON(msg)
}

// SendLastN tells the bridge how many remote video streams the client wants to receive.
func (c *Conn) SendLastN(n int) error {
	return c.SendJSON(map[string]any{
		"colibriClass": "LastNChangedEvent",
		"lastN":        n,
	})
}

// SendReceiverVideoConstraints announces detailed per-source video constraints to the bridge.
// Pass the constraints object verbatim (see Jitsi docs / ReceiverVideoConstraintsMessage).
func (c *Conn) SendReceiverVideoConstraints(constraints map[string]any) error {
	msg := map[string]any{
		"colibriClass": "ReceiverVideoConstraints",
	}
	for k, v := range constraints {
		if k == "colibriClass" {
			continue
		}
		msg[k] = v
	}
	return c.SendJSON(msg)
}

// SendVideoType signals the local video source type (camera/desktop/none).
//
// videoType: "camera", "desktop", or "none".
func (c *Conn) SendVideoType(videoType string) error {
	return c.SendJSON(map[string]any{
		"colibriClass": "VideoTypeMessage",
		"videoType":    videoType,
	})
}

// SendSourceVideoType signals the video type for a specific source.
func (c *Conn) SendSourceVideoType(sourceName, videoType string) error {
	return c.SendJSON(map[string]any{
		"colibriClass": "SourceVideoTypeMessage",
		"sourceName":   sourceName,
		"videoType":    videoType,
	})
}

// SendEndpointStats publishes endpoint statistics (bitrate, packet loss, RTT…) to the bridge.
// Pass the stats object verbatim — typical fields are "bitrate", "packetLoss", "jvbRTT", etc.
func (c *Conn) SendEndpointStats(stats map[string]any) error {
	msg := map[string]any{
		"colibriClass": "EndpointStats",
	}
	for k, v := range stats {
		if k == "colibriClass" {
			continue
		}
		msg[k] = v
	}
	return c.SendJSON(msg)
}

// SendReceiverAudioSubscription tells the bridge which audio sources to subscribe to.
//
// mode: "All" | "None" | "Include" | "Exclude"
// list: source names (ignored unless mode is Include/Exclude).
func (c *Conn) SendReceiverAudioSubscription(mode string, list []string) error {
	msg := map[string]any{
		"colibriClass": "ReceiverAudioSubscription",
		"mode":         mode,
	}
	if mode == "Include" || mode == "Exclude" {
		msg["list"] = list
	}
	return c.SendJSON(msg)
}
