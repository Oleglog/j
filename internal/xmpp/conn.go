package xmpp

import (
	"context"
	"crypto/sha1"
	"encoding/base64"
	"encoding/xml"
	"fmt"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
)

type Conn struct {
	ws        *websocket.Conn
	host      string
	room      string
	jid       string
	nick      string
	debug     bool
	mu        sync.Mutex
	ackH      atomic.Int64
	idSeq     atomic.Int64
	lastJngMu sync.Mutex
	lastJng   string
	occMu     sync.Mutex
	occupants map[string]struct{} // MUC nick → present (excluding self and "focus")
	stanzas   chan string
	jingles   chan string
	closed    chan struct{}
	closeOnce sync.Once

	// waitMu protects the per-stanza waiter maps below.
	waitMu sync.Mutex
	// iqWaiters resolves <iq type="result"/> or <iq type="error"/> back to
	// the caller of SendIQWait via stanza id. The chan delivers the full
	// stanza so the caller can inspect type/error payload.
	iqWaiters map[string]chan string
	// leaveWaiter fires when we observe our own presence-unavailable
	// echoed back by Prosody — the XMPP equivalent of MUC_LEFT used in
	// lib-jitsi-meet. Nil when no LeaveMUCWait is in flight.
	leaveWaiter chan struct{}
	// smAckWaiter fires when we receive a stream-management <a h=N/>
	// stanza. Used by the keepalive goroutine to detect a wedged or
	// silently-disconnected XMPP websocket: if our periodic <r/> doesn't
	// elicit a response, the connection is dead and we shut it down so
	// Prosody can drop us from the MUC promptly.
	smAckWaiter chan struct{}
}

type Service struct {
	Type      string
	Host      string
	Port      string
	Transport string
	Username  string
	Password  string
}

const jitsiCapsNode = "https://jitsi.org/jitsi-meet"

var jitsiMeetFeatures = []string{
	"http://jabber.org/protocol/caps",
	"http://jitsi.org/json-encoded-sources",
	"http://jitsi.org/receive-multiple-video-streams",
	"http://jitsi.org/remb",
	"http://jitsi.org/source-name",
	"http://jitsi.org/start-muted-room-metadata",
	"http://jitsi.org/tcc",
	"http://jitsi.org/visitors-1",
	"urn:ietf:rfc:4588",
	"urn:xmpp:jingle:1",
	"urn:xmpp:jingle:apps:dtls:0",
	"urn:xmpp:jingle:apps:rtp:1",
	"urn:xmpp:jingle:apps:rtp:audio",
	"urn:xmpp:jingle:apps:rtp:video",
	"urn:xmpp:jingle:transports:dtls-sctp:1",
	"urn:xmpp:jingle:transports:ice-udp:1",
}

var jitsiCapsVersion = calculateJitsiCapsVersion()

func Dial(ctx context.Context, host, room string, debug bool) (*Conn, error) {
	url := fmt.Sprintf("wss://%s/xmpp-websocket?room=%s", host, room)
	ws, _, err := websocket.Dial(ctx, url, &websocket.DialOptions{
		Subprotocols:    []string{"xmpp"},
		CompressionMode: websocket.CompressionContextTakeover,
		HTTPHeader: http.Header{
			"Accept":          {"*/*"},
			"Accept-Language": {"en-US,en;q=0.9"},
			"Cache-Control":   {"no-cache"},
			"Origin":          {"https://" + host},
			"Pragma":          {"no-cache"},
			"Sec-Fetch-Dest":  {"empty"},
			"Sec-Fetch-Mode":  {"websocket"},
			"Sec-Fetch-Site":  {"same-origin"},
			"User-Agent":      {"Mozilla/5.0 (X11; Linux x86_64; rv:150.0) Gecko/20100101 Firefox/150.0"},
		},
	})
	if err != nil {
		return nil, err
	}
	ws.SetReadLimit(1 << 20)

	c := &Conn{
		ws:        ws,
		host:      host,
		room:      room,
		debug:     debug,
		occupants: make(map[string]struct{}),
		stanzas:   make(chan string, 64),
		jingles:   make(chan string, 8),
		closed:    make(chan struct{}),
		iqWaiters: make(map[string]chan string),
	}

	if err := c.auth(ctx); err != nil {
		_ = ws.Close(websocket.StatusInternalError, "")
		return nil, err
	}

	go c.readLoop()
	go c.keepaliveLoop()
	return c, nil
}

func (c *Conn) JID() string  { return c.jid }
func (c *Conn) Nick() string { return c.nick }
func (c *Conn) Host() string { return c.host }
func (c *Conn) Room() string { return c.room }

// Send transmits an arbitrary XMPP stanza string. Caller is responsible for valid XML
// (and for adding xmlns="jabber:client" on iq/presence/message).
func (c *Conn) Send(s string) error { return c.send(s) }

// NextID returns a unique stanza id for outgoing IQs.
func (c *Conn) NextID() string {
	return fmt.Sprintf("j-%d", c.idSeq.Add(1))
}

// Stanzas returns the channel of incoming non-management XMPP stanzas.
func (c *Conn) Stanzas() <-chan string { return c.stanzas }

// keepaliveLoop periodically pokes the XMPP websocket with a stream
// management <r/> request and verifies that Prosody answers with an
// <a h=N/> ack. If three consecutive cycles fail to elicit an ack we
// declare the connection dead and shut it down.
//
// Why we need this: the e2e test fixture observed 90s windows where
// nothing flowed over XMPP because the application-level data carrier
// (seichannel) was wedged on RTP. In that quiet stretch, Prosody can
// drop us from the bind and our subsequent <presence type="unavailable"/>
// goes into a black hole, leaving ghost participants in the MUC for
// minutes — which is exactly the symptom we kept hitting on back-to-back
// runs of the same room. Keeping the channel pingable so that either the
// server keeps us alive or we detect death promptly mirrors what
// Strophe.js does for lib-jitsi-meet.
//
// Tunables: 30s between pings is well below typical server bind-idle
// timeouts (Prosody mod_smacks defaults to ~5 minutes) but high enough
// to be invisible in the protocol log. 10s ack window covers the
// worst-case meet.cryptopro.ru round trip we've measured.
func (c *Conn) keepaliveLoop() {
	const (
		interval = 30 * time.Second
		ackWait  = 10 * time.Second
	)
	t := time.NewTicker(interval)
	defer t.Stop()

	for {
		select {
		case <-c.closed:
			return
		case <-t.C:
		}

		// Arm an ack waiter before sending so a fast server reply can't
		// race us. If a previous cycle's waiter is still pending (which
		// shouldn't normally happen) we just keep using it.
		w := make(chan struct{})
		c.waitMu.Lock()
		if c.smAckWaiter == nil {
			c.smAckWaiter = w
		} else {
			w = c.smAckWaiter
		}
		c.waitMu.Unlock()

		if err := c.send(`<r xmlns="urn:xmpp:sm:3"/>`); err != nil {
			// send already marked the connection closed; just exit.
			return
		}

		select {
		case <-w:
			// Ack received; loop and wait for the next tick.
		case <-c.closed:
			return
		case <-time.After(ackWait):
			// No ack within the window — the websocket is wedged.
			// Shut it down so writers fail fast and Prosody sees the
			// underlying TCP go away (which prompts MUC cleanup on
			// the server side instead of waiting for its own idle
			// timeout, minutes from now).
			c.waitMu.Lock()
			if c.smAckWaiter == w {
				c.smAckWaiter = nil
			}
			c.waitMu.Unlock()
			c.markClosed()
			_ = c.ws.Close(websocket.StatusGoingAway, "keepalive timeout")
			return
		}
	}
}

func (c *Conn) Close() error {
	// markClosed is the only place that flips c.closed. Use a sync.Once
	// because both Close() and the readLoop's deferred close path can
	// race to mark the connection dead, and we want at most one close.
	c.markClosed()
	return c.ws.Close(websocket.StatusNormalClosure, "")
}

// markClosed signals all waiters (LeaveMUCWait, SendIQWait, keepalive,
// etc.) that the underlying websocket is no longer usable. Idempotent.
func (c *Conn) markClosed() {
	c.closeOnce.Do(func() { close(c.closed) })
}

func (c *Conn) send(s string) error {
	// Refuse writes once the connection has been declared dead so
	// callers see an immediate error instead of having their bytes
	// silently swallowed by a half-shut websocket. This is what makes
	// LeaveMUCWait return promptly on a dead Prosody link instead of
	// burning its full 5s timeout.
	select {
	case <-c.closed:
		return fmt.Errorf("xmpp connection closed")
	default:
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.debug {
		fmt.Fprintf(os.Stderr, "[xmpp] -> %s\n", s)
	}
	if err := c.ws.Write(context.Background(), websocket.MessageText, []byte(s)); err != nil {
		// Write failure means the websocket is gone. Mark closed so
		// any goroutine still blocked on c.closed wakes up.
		c.markClosed()
		return err
	}
	return nil
}

func (c *Conn) readOne(ctx context.Context) (string, error) {
	_, data, err := c.ws.Read(ctx)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func (c *Conn) readLoop() {
	// On exit (read error, server FIN, etc.) signal every waiter and
	// the keepalive goroutine that the connection is dead. Without
	// this, callers blocked in LeaveMUCWait / SendIQWait keep waiting
	// for stanzas that will never arrive.
	defer c.markClosed()
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
			_ = c.send(fmt.Sprintf(`<a h="%d" xmlns="urn:xmpp:sm:3"/>`, c.ackH.Load()))
			continue
		}
		if strings.HasPrefix(msg, "<a ") || strings.Contains(msg, "<a xmlns=\"urn:xmpp:sm:3\"") || strings.Contains(msg, "<a xmlns='urn:xmpp:sm:3'") {
			// Wake any pending keepalive ack waiter. The check inside
			// the lock keeps us from racing with keepalive setting up
			// a new waiter for the next cycle.
			c.waitMu.Lock()
			if w := c.smAckWaiter; w != nil {
				c.smAckWaiter = nil
				c.waitMu.Unlock()
				close(w)
			} else {
				c.waitMu.Unlock()
			}
			continue
		}
		c.ackH.Add(1)

		// Dispatch waiters before generic stanza fan-out. IQ result/error
		// is the XMPP-level ack for SendIQWait callers; own presence
		// unavailable echo is the MUC-level ack used by LeaveMUCWait. We
		// resolve them here so callers don't need to scan the stanzas
		// channel themselves.
		if isIQResultOrError(msg) {
			if id := extractXMLAttr(msg, "id"); id != "" {
				c.waitMu.Lock()
				if ch, ok := c.iqWaiters[id]; ok {
					delete(c.iqWaiters, id)
					c.waitMu.Unlock()
					select {
					case ch <- msg:
					default:
					}
					continue
				}
				c.waitMu.Unlock()
			}
		}

		// track MUC occupants from <presence> stanzas
		if strings.HasPrefix(msg, "<presence") || strings.HasPrefix(msg, "<presence ") {
			c.trackPresence(msg)
			if c.isOwnPresenceUnavailable(msg) {
				c.waitMu.Lock()
				if w := c.leaveWaiter; w != nil {
					c.leaveWaiter = nil
					c.waitMu.Unlock()
					close(w)
				} else {
					c.waitMu.Unlock()
				}
			}
		}

		// auto-reply to disco#info queries from Jicofo
		if isDiscoInfoGet(msg) {
			c.handleDiscoQuery(msg)
			continue
		}

		if isSessionInitiate(msg) {
			c.lastJngMu.Lock()
			c.lastJng = msg
			c.lastJngMu.Unlock()
			select {
			case c.jingles <- msg:
			default:
			}
		}

		select {
		case c.stanzas <- msg:
		case <-c.closed:
			return
		}
	}
}

// trackPresence updates the occupants map from a <presence> stanza.
// Available → add, type="unavailable" → remove. Skips self and "focus".
func (c *Conn) trackPresence(msg string) {
	from := extractXMLAttr(msg, "from")
	if from == "" {
		return
	}
	// from = "room@conference.host/<nick>"
	slash := strings.LastIndex(from, "/")
	if slash < 0 {
		return
	}
	nick := from[slash+1:]
	if nick == "" || nick == "focus" || nick == c.nick {
		return
	}
	// also skip if not from our MUC room
	if !strings.HasPrefix(from, c.room+"@") {
		return
	}

	c.occMu.Lock()
	defer c.occMu.Unlock()
	if strings.Contains(msg, `type='unavailable'`) || strings.Contains(msg, `type="unavailable"`) {
		delete(c.occupants, nick)
	} else {
		c.occupants[nick] = struct{}{}
	}
}

// Occupants returns the list of MUC nicks (other participants) currently in the room.
// "focus" and self are excluded. Order is unspecified.
func (c *Conn) Occupants() []string {
	c.occMu.Lock()
	defer c.occMu.Unlock()
	out := make([]string, 0, len(c.occupants))
	for n := range c.occupants {
		out = append(out, n)
	}
	return out
}

func (c *Conn) handleDiscoQuery(msg string) {
	from := extractXMLAttr(msg, "from")
	id := extractXMLAttr(msg, "id")
	if from == "" || id == "" {
		return
	}
	resp := fmt.Sprintf(`<iq to="%s" id="%s" type="result" xmlns="jabber:client"><query xmlns="http://jabber.org/protocol/disco#info">%s</query></iq>`,
		from, id, discoFeatureXML())
	_ = c.send(resp)
}

func isDiscoInfoGet(msg string) bool {
	return strings.Contains(msg, "disco#info") && extractXMLAttr(msg, "type") == "get"
}

func isSessionInitiate(msg string) bool {
	return strings.Contains(msg, "jingle") && strings.Contains(msg, "session-initiate")
}

func discoFeatureXML() string {
	var b strings.Builder
	for _, feature := range sortedJitsiMeetFeatures() {
		fmt.Fprintf(&b, `<feature var="%s"/>`, feature)
	}
	return b.String()
}

func calculateJitsiCapsVersion() string {
	var s strings.Builder
	for _, feature := range sortedJitsiMeetFeatures() {
		s.WriteString(feature)
		s.WriteByte('<')
	}
	sum := sha1.Sum([]byte(s.String()))
	return base64.StdEncoding.EncodeToString(sum[:])
}

func sortedJitsiMeetFeatures() []string {
	features := append([]string(nil), jitsiMeetFeatures...)
	sort.Strings(features)
	return features
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

func (c *Conn) DiscoverServices(ctx context.Context) ([]Service, error) {
	iq := fmt.Sprintf(`<iq type="get" to="%s" id="disco_1" xmlns="jabber:client"><services xmlns="urn:xmpp:extdisco:2"/></iq>`, c.host)
	if err := c.send(iq); err != nil {
		return nil, err
	}
	return c.waitServices(ctx)
}

func (c *Conn) waitServices(ctx context.Context) ([]Service, error) {
	for {
		select {
		case msg := <-c.stanzas:
			if strings.Contains(msg, "urn:xmpp:extdisco:2") {
				return parseServices(msg), nil
			}
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-c.closed:
			return nil, fmt.Errorf("connection closed")
		}
	}
}

func (c *Conn) AllocateFocus(ctx context.Context, room string) error {
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
		case <-ctx.Done():
			return ctx.Err()
		case <-c.closed:
			return fmt.Errorf("connection closed")
		}
	}
}

func (c *Conn) JoinMUC(ctx context.Context, room, displayName string) error {
	roomJID := fmt.Sprintf("%s@conference.%s/%s", room, c.host, c.nick)
	presence := fmt.Sprintf(`<presence to="%s" xmlns="jabber:client"><x xmlns="http://jabber.org/protocol/muc"/><stats-id>%s</stats-id><c hash="sha-1" node="%s" ver="%s" xmlns="http://jabber.org/protocol/caps"/><SourceInfo>{}</SourceInfo><jitsi_participant_codecList>vp8,h264,av1,vp9</jitsi_participant_codecList><nick xmlns="http://jabber.org/protocol/nick">%s</nick></presence>`,
		roomJID, displayName[:min(3, len(displayName))]+"-j", jitsiCapsNode, jitsiCapsVersion, displayName)
	if err := c.send(presence); err != nil {
		return err
	}
	// wait for self-presence (status 110)
	for {
		select {
		case msg := <-c.stanzas:
			if strings.Contains(msg, "status code=\"110\"") || strings.Contains(msg, `code='110'`) {
				_ = c.SendRoomInfo(room)
				return nil
			}
		case <-ctx.Done():
			return ctx.Err()
		case <-c.closed:
			return fmt.Errorf("connection closed")
		}
	}
}

func (c *Conn) SendRoomInfo(room string) error {
	roomBareJID := fmt.Sprintf("%s@conference.%s", room, c.host)
	id := c.NextID()
	iq := fmt.Sprintf(`<iq to="%s" type="get" id="%s" xmlns="jabber:client"><query xmlns="http://jabber.org/protocol/disco#info"/></iq>`, roomBareJID, id)
	return c.send(iq)
}

func (c *Conn) WaitJingle(ctx context.Context) (string, error) {
	for {
		select {
		case msg := <-c.jingles:
			return msg, nil
		case <-ctx.Done():
			return "", ctx.Err()
		case <-c.closed:
			return "", fmt.Errorf("connection closed")
		}
	}
}

// LastJingleStanza returns the most recently received Jingle session-initiate raw stanza.
func (c *Conn) LastJingleStanza() string {
	c.lastJngMu.Lock()
	defer c.lastJngMu.Unlock()
	return c.lastJng
}

func (c *Conn) SendSessionAccept(sid, initiator, roomJID, sdp string) error {
	iq := fmt.Sprintf(`<iq to="%s" type="set" id="accept_1" xmlns="jabber:client"><jingle xmlns="urn:xmpp:jingle:1" action="session-accept" sid="%s" initiator="%s" responder="%s">%s</jingle></iq>`,
		roomJID+"/focus", sid, initiator, c.jid, sdp)
	return c.send(iq)
}

// SendJingle sends an arbitrary Jingle action (transport-info, source-add, source-remove,
// session-terminate, …). innerXML is the body inside <jingle …>.
func (c *Conn) SendJingle(to, action, sid, initiator string, innerXML string) error {
	id := c.NextID()
	iq := fmt.Sprintf(
		`<iq to="%s" type="set" id="%s" xmlns="jabber:client"><jingle xmlns="urn:xmpp:jingle:1" action="%s" sid="%s" initiator="%s" responder="%s">%s</jingle></iq>`,
		to, id, action, sid, initiator, c.jid, innerXML)
	return c.send(iq)
}

// SendJingleWait sends a Jingle IQ and waits until the recipient acknowledges
// it with a matching <iq type="result"/> or <iq type="error"/>.
func (c *Conn) SendJingleWait(to, action, sid, initiator string, innerXML string, timeout time.Duration) (string, error) {
	id := c.NextID()
	iq := fmt.Sprintf(
		`<iq to="%s" type="set" id="%s" xmlns="jabber:client"><jingle xmlns="urn:xmpp:jingle:1" action="%s" sid="%s" initiator="%s" responder="%s">%s</jingle></iq>`,
		to, id, action, sid, initiator, c.jid, innerXML)
	return c.SendIQWait(iq, id, timeout)
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

// LeaveMUCWait sends MUC presence unavailable and waits for Prosody to echo
// it back (the same handshake lib-jitsi-meet uses via XMPPEvents.MUC_LEFT).
// Returning nil means the server has acknowledged our exit and routed it on
// to Jicofo; the bridge slot can be reclaimed before this function returns.
// Times out at the supplied deadline so a wedged server never hangs callers.
func (c *Conn) LeaveMUCWait(room string, timeout time.Duration) error {
	w := make(chan struct{})
	c.waitMu.Lock()
	// A second concurrent leave (shouldn't normally happen) just inherits
	// the existing waiter; we don't try to chain them.
	if c.leaveWaiter == nil {
		c.leaveWaiter = w
	} else {
		w = c.leaveWaiter
	}
	c.waitMu.Unlock()

	roomJID := fmt.Sprintf("%s@conference.%s/%s", room, c.host, c.nick)
	if err := c.send(fmt.Sprintf(`<presence to="%s" type="unavailable" xmlns="jabber:client"/>`, roomJID)); err != nil {
		c.waitMu.Lock()
		if c.leaveWaiter == w {
			c.leaveWaiter = nil
		}
		c.waitMu.Unlock()
		return err
	}

	select {
	case <-w:
		return nil
	case <-c.closed:
		return fmt.Errorf("connection closed before MUC leave confirmed")
	case <-time.After(timeout):
		c.waitMu.Lock()
		if c.leaveWaiter == w {
			c.leaveWaiter = nil
		}
		c.waitMu.Unlock()
		return fmt.Errorf("timeout waiting for MUC leave confirmation")
	}
}

// SendIQWait sends an IQ and waits for a matching <iq type="result"/> or
// <iq type="error"/> keyed by stanza id. Used for fire-and-confirm flows
// like session-terminate where the caller needs to know the server has
// accepted the request before continuing tear-down.
func (c *Conn) SendIQWait(iqXML, id string, timeout time.Duration) (string, error) {
	if id == "" {
		return "", fmt.Errorf("SendIQWait requires non-empty id")
	}
	ch := make(chan string, 1)
	c.waitMu.Lock()
	c.iqWaiters[id] = ch
	c.waitMu.Unlock()

	if err := c.send(iqXML); err != nil {
		c.waitMu.Lock()
		delete(c.iqWaiters, id)
		c.waitMu.Unlock()
		return "", err
	}

	select {
	case reply := <-ch:
		return reply, nil
	case <-c.closed:
		c.waitMu.Lock()
		delete(c.iqWaiters, id)
		c.waitMu.Unlock()
		return "", fmt.Errorf("connection closed before IQ %s reply", id)
	case <-time.After(timeout):
		c.waitMu.Lock()
		delete(c.iqWaiters, id)
		c.waitMu.Unlock()
		return "", fmt.Errorf("timeout waiting for IQ %s reply", id)
	}
}

// isIQResultOrError tells whether a stanza is an IQ acknowledging an earlier
// IQ we sent. Used in the read loop to dispatch SendIQWait callers.
func isIQResultOrError(msg string) bool {
	if !strings.HasPrefix(msg, "<iq") {
		return false
	}
	// type attribute is small and appears near the front of the iq element
	t := extractXMLAttr(msg, "type")
	return t == "result" || t == "error"
}

// isOwnPresenceUnavailable matches the broadcast Prosody sends back to us
// when our MUC presence unavailable has been processed: from is our own
// MUC JID with type="unavailable". This is what fires LeaveMUCWait.
func (c *Conn) isOwnPresenceUnavailable(msg string) bool {
	if !strings.Contains(msg, `type='unavailable'`) && !strings.Contains(msg, `type="unavailable"`) {
		return false
	}
	from := extractXMLAttr(msg, "from")
	if from == "" || c.nick == "" || c.room == "" {
		return false
	}
	want := fmt.Sprintf("%s@conference.%s/%s", c.room, c.host, c.nick)
	return from == want
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
	_ = xml.Unmarshal([]byte(s), &iq)

	var result []Service
	for _, svc := range iq.Services.Services {
		result = append(result, Service(svc))
	}
	return result
}

func xmlEscape(s string) string {
	var b strings.Builder
	_ = xml.EscapeText(&b, []byte(s))
	return b.String()
}
