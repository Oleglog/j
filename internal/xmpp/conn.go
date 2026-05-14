package xmpp

import (
	"context"
	"encoding/xml"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/coder/websocket"
)

type Conn struct {
	ws      *websocket.Conn
	host    string
	room    string
	jid     string
	nick    string
	mu      sync.Mutex
	ackH    atomic.Int64
	stanzas chan string
	closed  chan struct{}
}

type Service struct {
	Type      string
	Host      string
	Port      string
	Transport string
	Username  string
	Password  string
}

func Dial(ctx context.Context, host, room string) (*Conn, error) {
	url := fmt.Sprintf("wss://%s/xmpp-websocket?room=%s", host, room)
	ws, _, err := websocket.Dial(ctx, url, &websocket.DialOptions{
		Subprotocols: []string{"xmpp"},
	})
	if err != nil {
		return nil, err
	}
	ws.SetReadLimit(1 << 20)

	c := &Conn{
		ws:      ws,
		host:    host,
		room:    room,
		stanzas: make(chan string, 64),
		closed:  make(chan struct{}),
	}

	if err := c.auth(ctx); err != nil {
		ws.Close(websocket.StatusInternalError, "")
		return nil, err
	}

	go c.readLoop()
	return c, nil
}

func (c *Conn) JID() string  { return c.jid }
func (c *Conn) Nick() string { return c.nick }

func (c *Conn) Close() error {
	select {
	case <-c.closed:
	default:
		close(c.closed)
	}
	return c.ws.Close(websocket.StatusNormalClosure, "")
}

func (c *Conn) send(s string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.ws.Write(context.Background(), websocket.MessageText, []byte(s))
}

func (c *Conn) readOne(ctx context.Context) (string, error) {
	_, data, err := c.ws.Read(ctx)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func (c *Conn) readLoop() {
	for {
		select {
		case <-c.closed:
			return
		default:
		}
		msg, err := c.readOne(context.Background())
		if err != nil {
			return
		}
		// handle stream management
		if strings.Contains(msg, "<r ") || strings.Contains(msg, "<r/>") {
			c.send(fmt.Sprintf(`<a h="%d" xmlns="urn:xmpp:sm:3"/>`, c.ackH.Load()))
			continue
		}
		if strings.Contains(msg, "<a ") {
			continue
		}
		c.ackH.Add(1)
		select {
		case c.stanzas <- msg:
		case <-c.closed:
			return
		}
	}
}

func (c *Conn) auth(ctx context.Context) error {
	// open stream
	open := fmt.Sprintf(`<open to="%s" version="1.0" xmlns="urn:ietf:params:xml:ns:xmpp-framing"/>`, c.host)
	if err := c.send(open); err != nil {
		return err
	}
	// read features
	if _, err := c.readOne(ctx); err != nil {
		return err
	}
	if _, err := c.readOne(ctx); err != nil {
		return err
	}

	// ANONYMOUS SASL
	if err := c.send(`<auth mechanism="ANONYMOUS" xmlns="urn:ietf:params:xml:ns:xmpp-sasl"/>`); err != nil {
		return err
	}
	resp, err := c.readOne(ctx)
	if err != nil {
		return err
	}
	if !strings.Contains(resp, "<success") {
		return fmt.Errorf("sasl failed: %s", resp)
	}

	// reopen stream
	if err := c.send(open); err != nil {
		return err
	}
	if _, err := c.readOne(ctx); err != nil {
		return err
	}
	if _, err := c.readOne(ctx); err != nil {
		return err
	}

	// bind
	if err := c.send(`<iq type="set" id="bind_1"><bind xmlns="urn:ietf:params:xml:ns:xmpp-bind"/></iq>`); err != nil {
		return err
	}
	bindResp, err := c.readOne(ctx)
	if err != nil {
		return err
	}
	c.jid = extractJID(bindResp)
	if c.jid == "" {
		return fmt.Errorf("bind failed: %s", bindResp)
	}
	// nick = first 8 chars of uuid part
	parts := strings.Split(c.jid, "@")
	if len(parts) > 0 && len(parts[0]) >= 8 {
		c.nick = parts[0][:8]
	}

	// session
	if err := c.send(`<iq type="set" id="sess_1"><session xmlns="urn:ietf:params:xml:ns:xmpp-session"/></iq>`); err != nil {
		return err
	}
	if _, err := c.readOne(ctx); err != nil {
		return err
	}

	// enable stream management
	if err := c.send(`<enable resume="true" xmlns="urn:xmpp:sm:3"/>`); err != nil {
		return err
	}
	if _, err := c.readOne(ctx); err != nil {
		return err
	}

	return nil
}

func (c *Conn) DiscoverServices() ([]Service, error) {
	iq := fmt.Sprintf(`<iq type="get" to="%s" id="disco_1"><services xmlns="urn:xmpp:extdisco:2"/></iq>`, c.host)
	if err := c.send(iq); err != nil {
		return nil, err
	}
	return c.waitServices()
}

func (c *Conn) waitServices() ([]Service, error) {
	for {
		select {
		case msg := <-c.stanzas:
			if strings.Contains(msg, "urn:xmpp:extdisco:2") {
				return parseServices(msg), nil
			}
		case <-c.closed:
			return nil, fmt.Errorf("connection closed")
		}
	}
}

func (c *Conn) AllocateFocus(room string) error {
	roomJID := fmt.Sprintf("%s@conference.%s", room, c.host)
	iq := fmt.Sprintf(`<iq to="focus.%s" type="set" id="focus_1"><conference room="%s" machine-uid="%s" xmlns="http://jitsi.org/protocol/focus"><property name="rtcstatsEnabled" value="false"/><property name="visitors-version" value="1"/></conference></iq>`,
		c.host, roomJID, c.nick)
	if err := c.send(iq); err != nil {
		return err
	}
	// wait for focus response
	for {
		select {
		case msg := <-c.stanzas:
			if strings.Contains(msg, "conference") && strings.Contains(msg, "ready") {
				return nil
			}
			if strings.Contains(msg, "type=\"error\"") && strings.Contains(msg, "focus_1") {
				return fmt.Errorf("focus allocation failed: %s", msg)
			}
		case <-c.closed:
			return fmt.Errorf("connection closed")
		}
	}
}

func (c *Conn) JoinMUC(room, displayName string) error {
	roomJID := fmt.Sprintf("%s@conference.%s/%s", room, c.host, c.nick)
	presence := fmt.Sprintf(`<presence to="%s"><x xmlns="http://jabber.org/protocol/muc"/><stats-id>%s</stats-id><c hash="sha-1" node="https://jitsi.org/jitsi-meet" ver="location" xmlns="http://jabber.org/protocol/caps"/><SourceInfo>{}</SourceInfo><jitsi_participant_codecList>vp9,vp8,h264</jitsi_participant_codecList><nick xmlns="http://jabber.org/protocol/nick">%s</nick></presence>`,
		roomJID, displayName[:min(3, len(displayName))]+"-j", displayName)
	if err := c.send(presence); err != nil {
		return err
	}
	// wait for self-presence (status 110)
	for {
		select {
		case msg := <-c.stanzas:
			if strings.Contains(msg, "status code=\"110\"") || strings.Contains(msg, `code='110'`) {
				return nil
			}
		case <-c.closed:
			return fmt.Errorf("connection closed")
		}
	}
}

func (c *Conn) WaitJingle(ctx context.Context) (string, error) {
	for {
		select {
		case msg := <-c.stanzas:
			if strings.Contains(msg, "jingle") && strings.Contains(msg, "session-initiate") {
				return msg, nil
			}
		case <-ctx.Done():
			return "", ctx.Err()
		case <-c.closed:
			return "", fmt.Errorf("connection closed")
		}
	}
}

func (c *Conn) SendSessionAccept(sid, initiator, roomJID, sdp string) error {
	// sdp is already formatted as jingle XML by caller
	iq := fmt.Sprintf(`<iq to="%s" type="set" id="accept_1"><jingle xmlns="urn:xmpp:jingle:1" action="session-accept" sid="%s" initiator="%s" responder="%s">%s</jingle></iq>`,
		roomJID+"/focus", sid, initiator, c.jid, sdp)
	return c.send(iq)
}

func (c *Conn) SendGroupchat(roomJID, body string) error {
	msg := fmt.Sprintf(`<message to="%s" type="groupchat"><body>%s</body></message>`, roomJID, xmlEscape(body))
	return c.send(msg)
}

func extractJID(s string) string {
	start := strings.Index(s, "<jid>")
	if start == -1 {
		return ""
	}
	start += 5
	end := strings.Index(s[start:], "</jid>")
	if end == -1 {
		return ""
	}
	return s[start : start+end]
}

func parseServices(s string) []Service {
	type xmlService struct {
		Type      string `xml:"type,attr"`
		Host      string `xml:"host,attr"`
		Port      string `xml:"port,attr"`
		Transport string `xml:"transport,attr"`
		Username  string `xml:"username,attr"`
		Password  string `xml:"password,attr"`
	}
	type xmlServices struct {
		Services []xmlService `xml:"service"`
	}
	type xmlIQ struct {
		Services xmlServices `xml:"services"`
	}

	var iq xmlIQ
	xml.Unmarshal([]byte(s), &iq)

	var result []Service
	for _, svc := range iq.Services.Services {
		result = append(result, Service{
			Type:      svc.Type,
			Host:      svc.Host,
			Port:      svc.Port,
			Transport: svc.Transport,
			Username:  svc.Username,
			Password:  svc.Password,
		})
	}
	return result
}

func xmlEscape(s string) string {
	var b strings.Builder
	xml.EscapeText(&b, []byte(s))
	return b.String()
}
