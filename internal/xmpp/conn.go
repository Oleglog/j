package xmpp

import (
	"context"
	"encoding/xml"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
)

type Conn struct {
	ws      *websocket.Conn
	host    string
	room    string
	jid     string
	nick    string
	debug   bool
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

func Dial(ctx context.Context, host, room string, debug bool) (*Conn, error) {
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
		debug:   debug,
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

// Stanzas returns the channel of incoming non-management XMPP stanzas.
func (c *Conn) Stanzas() <-chan string { return c.stanzas }

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
	if c.debug {
		fmt.Fprintf(os.Stderr, "[xmpp] -> %s\n", s)
	}
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
		if c.debug {
			fmt.Fprintf(os.Stderr, "[xmpp:loop] <- %s\n", msg)
		}
		// handle stream management
		if strings.Contains(msg, "<r ") || strings.Contains(msg, "<r/>") || strings.Contains(msg, "<r xmlns") {
			c.send(fmt.Sprintf(`<a h="%d" xmlns="urn:xmpp:sm:3"/>`, c.ackH.Load()))
			continue
		}
		if strings.HasPrefix(msg, "<a ") || strings.Contains(msg, "<a xmlns=\"urn:xmpp:sm:3\"") || strings.Contains(msg, "<a xmlns='urn:xmpp:sm:3'") {
			continue
		}
		c.ackH.Add(1)

		// auto-reply to disco#info queries from Jicofo
		if strings.Contains(msg, "disco#info") && strings.Contains(msg, "type='get'") {
			c.handleDiscoQuery(msg)
			continue
		}

		select {
		case c.stanzas <- msg:
		case <-c.closed:
			return
		}
	}
}

func (c *Conn) handleDiscoQuery(msg string) {
	from := extractXMLAttr(msg, "from")
	id := extractXMLAttr(msg, "id")
	if from == "" || id == "" {
		return
	}
	resp := fmt.Sprintf(`<iq to="%s" id="%s" type="result" xmlns="jabber:client"><query xmlns="http://jabber.org/protocol/disco#info"><feature var="urn:xmpp:jingle:1"/><feature var="urn:xmpp:jingle:apps:rtp:1"/><feature var="urn:xmpp:jingle:transports:ice-udp:1"/><feature var="urn:xmpp:jingle:apps:dtls:0"/><feature var="urn:xmpp:jingle:transports:dtls-sctp:1"/><feature var="urn:xmpp:jingle:apps:rtp:audio"/><feature var="urn:xmpp:jingle:apps:rtp:video"/><feature var="http://jitsi.org/protocol/colibri2"/></query></iq>`, from, id)
	c.send(resp)
}

func extractXMLAttr(s, attr string) string {
	// try single quotes first (prosody style)
	key := attr + "='"
	i := strings.Index(s, key)
	if i != -1 {
		i += len(key)
		end := strings.IndexByte(s[i:], '\'')
		if end != -1 {
			return s[i : i+end]
		}
	}
	// try double quotes
	key = attr + `="`
	i = strings.Index(s, key)
	if i != -1 {
		i += len(key)
		end := strings.IndexByte(s[i:], '"')
		if end != -1 {
			return s[i : i+end]
		}
	}
	return ""
}

func (c *Conn) auth(ctx context.Context) error {
	open := fmt.Sprintf(`<open to="%s" version="1.0" xmlns="urn:ietf:params:xml:ns:xmpp-framing"/>`, c.host)

	// phase 1: open stream
	if err := c.send(open); err != nil {
		return err
	}
	// read until we get stream features (server may send open + features separately or together)
	if err := c.readUntil(ctx, "features"); err != nil {
		return fmt.Errorf("initial features: %w", err)
	}

	// ANONYMOUS SASL
	if err := c.send(`<auth mechanism="ANONYMOUS" xmlns="urn:ietf:params:xml:ns:xmpp-sasl"/>`); err != nil {
		return err
	}
	if err := c.readUntil(ctx, "success"); err != nil {
		return fmt.Errorf("sasl: %w", err)
	}

	// phase 2: reopen stream after SASL
	if err := c.send(open); err != nil {
		return err
	}
	if err := c.readUntil(ctx, "features"); err != nil {
		return fmt.Errorf("post-auth features: %w", err)
	}

	// bind
	if err := c.send(`<iq type="set" id="bind_1" xmlns="jabber:client"><bind xmlns="urn:ietf:params:xml:ns:xmpp-bind"/></iq>`); err != nil {
		return err
	}
	bindResp, err := c.readUntilReturn(ctx, "<jid>")
	if err != nil {
		return fmt.Errorf("bind: %w", err)
	}
	c.jid = extractJID(bindResp)
	if c.jid == "" {
		return fmt.Errorf("bind failed: %s", bindResp)
	}
	parts := strings.Split(c.jid, "@")
	if len(parts) > 0 && len(parts[0]) >= 8 {
		c.nick = parts[0][:8]
	}

	// session
	if err := c.send(`<iq type="set" id="sess_1" xmlns="jabber:client"><session xmlns="urn:ietf:params:xml:ns:xmpp-session"/></iq>`); err != nil {
		return err
	}
	if err := c.readUntil(ctx, "sess_1"); err != nil {
		return fmt.Errorf("session: %w", err)
	}

	// enable stream management
	if err := c.send(`<enable resume="true" xmlns="urn:xmpp:sm:3"/>`); err != nil {
		return err
	}
	if err := c.readUntil(ctx, "enabled"); err != nil {
		return fmt.Errorf("sm enable: %w", err)
	}

	return nil
}

func (c *Conn) readUntil(ctx context.Context, substr string) error {
	for {
		msg, err := c.readOne(ctx)
		if err != nil {
			return err
		}
		if c.debug {
			fmt.Fprintf(os.Stderr, "[xmpp] <- %s\n", msg)
		}
		if strings.Contains(msg, substr) {
			return nil
		}
		if strings.Contains(msg, "stream:error") || strings.Contains(msg, "<failure") {
			return fmt.Errorf("server error: %s", msg)
		}
	}
}

func (c *Conn) readUntilReturn(ctx context.Context, substr string) (string, error) {
	for {
		msg, err := c.readOne(ctx)
		if err != nil {
			return "", err
		}
		if c.debug {
			fmt.Fprintf(os.Stderr, "[xmpp] <- %s\n", msg)
		}
		if strings.Contains(msg, substr) {
			return msg, nil
		}
		if strings.Contains(msg, "stream:error") || strings.Contains(msg, "<failure") {
			return "", fmt.Errorf("server error: %s", msg)
		}
	}
}

func (c *Conn) DiscoverServices() ([]Service, error) {
	iq := fmt.Sprintf(`<iq type="get" to="%s" id="disco_1" xmlns="jabber:client"><services xmlns="urn:xmpp:extdisco:2"/></iq>`, c.host)
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
	iq := fmt.Sprintf(`<iq to="focus.%s" type="set" id="focus_1" xmlns="jabber:client"><conference room="%s" machine-uid="%s" xmlns="http://jitsi.org/protocol/focus"><property name="rtcstatsEnabled" value="false"/><property name="visitors-version" value="1"/></conference></iq>`,
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
	presence := fmt.Sprintf(`<presence to="%s" xmlns="jabber:client"><x xmlns="http://jabber.org/protocol/muc"/><stats-id>%s</stats-id><c hash="sha-1" node="https://jitsi.org/jitsi-meet" ver="location" xmlns="http://jabber.org/protocol/caps"/><SourceInfo>{}</SourceInfo><jitsi_participant_codecList>vp9,vp8,h264</jitsi_participant_codecList><nick xmlns="http://jabber.org/protocol/nick">%s</nick></presence>`,
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
	iq := fmt.Sprintf(`<iq to="%s" type="set" id="accept_1" xmlns="jabber:client"><jingle xmlns="urn:xmpp:jingle:1" action="session-accept" sid="%s" initiator="%s" responder="%s">%s</jingle></iq>`,
		roomJID+"/focus", sid, initiator, c.jid, sdp)
	return c.send(iq)
}

func (c *Conn) SendGroupchat(roomJID, body string) error {
	msg := fmt.Sprintf(`<message to="%s" type="groupchat" xmlns="jabber:client"><body>%s</body></message>`, roomJID, xmlEscape(body))
	return c.send(msg)
}

func (c *Conn) RaiseHand(room string) error {
	roomJID := fmt.Sprintf("%s@conference.%s/%s", room, c.host, c.nick)
	ts := fmt.Sprintf("%d", time.Now().UnixMilli())
	return c.send(fmt.Sprintf(`<presence to="%s" xmlns="jabber:client"><jitsi_participant_raisedHand>%s</jitsi_participant_raisedHand></presence>`, roomJID, ts))
}

func (c *Conn) LowerHand(room string) error {
	roomJID := fmt.Sprintf("%s@conference.%s/%s", room, c.host, c.nick)
	return c.send(fmt.Sprintf(`<presence to="%s" xmlns="jabber:client"><jitsi_participant_raisedHand/></presence>`, roomJID))
}

func (c *Conn) LeaveMUC(room string) error {
	roomJID := fmt.Sprintf("%s@conference.%s/%s", room, c.host, c.nick)
	return c.send(fmt.Sprintf(`<presence to="%s" type="unavailable" xmlns="jabber:client"/>`, roomJID))
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
