// Package peer bridges a pion *webrtc.PeerConnection with a Jicofo Jingle session.
// It is intentionally low-level: caller owns the PC and does whatever they want with
// tracks/data channels. peer just wires up SDP↔Jingle conversion and signalling.
package peer

import (
	"context"
	"fmt"
	"strings"

	"github.com/pion/webrtc/v4"
	"github.com/zarazaex69/j/internal/jingle"
	"github.com/zarazaex69/j/internal/xmpp"
)

// Negotiator handles a single Jingle session against Jicofo using a pion PeerConnection.
type Negotiator struct {
	XMPP        *xmpp.Conn
	JingleStanza string             // raw <iq…><jingle action="session-initiate"…>…</jingle></iq>
	RoomJID     string              // <room>@conference.<host>
	PC          *webrtc.PeerConnection
	OnRemote    func(track *webrtc.TrackRemote, recv *webrtc.RTPReceiver) // optional
	OnIceConnectionStateChange func(webrtc.ICEConnectionState)

	parsed *jingle.XMLJingle
}

func (n *Negotiator) ensureParsed() error {
	if n.parsed != nil {
		return nil
	}
	if n.JingleStanza == "" {
		return fmt.Errorf("peer: JingleStanza is empty")
	}
	jng, err := jingle.ParseStanza(n.JingleStanza)
	if err != nil || jng == nil {
		return fmt.Errorf("peer: parse jingle: %w", err)
	}
	n.parsed = jng
	return nil
}

// SID returns the Jingle session id.
func (n *Negotiator) SID() string {
	if n.ensureParsed() != nil {
		return ""
	}
	return n.parsed.SID
}

// Accept performs SetRemoteDescription(offer) → CreateAnswer → SetLocalDescription(answer)
// → wait for ICE gathering complete → SendSessionAccept to Jicofo.
//
// Caller should have configured PC's transceivers/datachannels BEFORE calling Accept.
func (n *Negotiator) Accept(ctx context.Context) error {
	if n.PC == nil || n.XMPP == nil {
		return fmt.Errorf("peer: PC and XMPP must be set")
	}
	if err := n.ensureParsed(); err != nil {
		return err
	}

	if n.OnRemote != nil {
		n.PC.OnTrack(n.OnRemote)
	}
	if n.OnIceConnectionStateChange != nil {
		n.PC.OnICEConnectionStateChange(n.OnIceConnectionStateChange)
	}

	offerSDP := jingle.JingleToSDP(n.parsed)
	if err := n.PC.SetRemoteDescription(webrtc.SessionDescription{
		Type: webrtc.SDPTypeOffer,
		SDP:  offerSDP,
	}); err != nil {
		return fmt.Errorf("set remote desc: %w", err)
	}

	answer, err := n.PC.CreateAnswer(nil)
	if err != nil {
		return fmt.Errorf("create answer: %w", err)
	}
	if err := n.PC.SetLocalDescription(answer); err != nil {
		return fmt.Errorf("set local desc: %w", err)
	}

	select {
	case <-webrtc.GatheringCompletePromise(n.PC):
	case <-ctx.Done():
		return ctx.Err()
	}

	final := n.PC.LocalDescription().SDP
	jingleAccept := jingle.SDPToJingleAccept(final)

	return n.XMPP.SendSessionAccept(n.parsed.SID, n.parsed.Initiator, n.RoomJID, jingleAccept)
}

// SendTransportInfo announces additional ICE candidates to Jicofo (trickle ICE).
// Pass an SDP-style candidate line (without the leading "a=").
func (n *Negotiator) SendTransportInfo(mediaName, candidateLine string) error {
	if err := n.ensureParsed(); err != nil {
		return err
	}
	cand := buildJingleCandidateXML(candidateLine)
	inner := fmt.Sprintf(
		`<content creator="responder" name="%s"><transport xmlns="urn:xmpp:jingle:transports:ice-udp:1">%s</transport></content>`,
		mediaName, cand)
	return n.XMPP.SendJingle(n.RoomJID+"/focus", "transport-info", n.parsed.SID, n.parsed.Initiator, inner)
}

// SendSourceAdd announces local SSRC sources to Jicofo (after adding tracks).
func (n *Negotiator) SendSourceAdd(sourcesXML string) error {
	if err := n.ensureParsed(); err != nil {
		return err
	}
	return n.XMPP.SendJingle(n.RoomJID+"/focus", "source-add", n.parsed.SID, n.parsed.Initiator, sourcesXML)
}

// SendSourceRemove removes previously announced sources.
func (n *Negotiator) SendSourceRemove(sourcesXML string) error {
	if err := n.ensureParsed(); err != nil {
		return err
	}
	return n.XMPP.SendJingle(n.RoomJID+"/focus", "source-remove", n.parsed.SID, n.parsed.Initiator, sourcesXML)
}

// Terminate sends session-terminate to gracefully end the Jingle session.
func (n *Negotiator) Terminate(reason string) error {
	if err := n.ensureParsed(); err != nil {
		return err
	}
	if reason == "" {
		reason = "success"
	}
	inner := fmt.Sprintf(`<reason><%s/></reason>`, reason)
	return n.XMPP.SendJingle(n.RoomJID+"/focus", "session-terminate", n.parsed.SID, n.parsed.Initiator, inner)
}

// buildJingleCandidateXML converts an SDP candidate line to <candidate .../>
func buildJingleCandidateXML(raw string) string {
	raw = strings.TrimPrefix(raw, "a=")
	raw = strings.TrimPrefix(raw, "candidate:")
	fs := strings.Fields(raw)
	if len(fs) < 8 {
		return ""
	}
	foundation := fs[0]
	component := fs[1]
	protocol := fs[2]
	priority := fs[3]
	ip := fs[4]
	port := fs[5]
	candType := fs[7]
	var raddr, rport, generation, tcptype string
	for i := 8; i+1 < len(fs); i += 2 {
		switch fs[i] {
		case "raddr":
			raddr = fs[i+1]
		case "rport":
			rport = fs[i+1]
		case "generation":
			generation = fs[i+1]
		case "tcptype":
			tcptype = fs[i+1]
		}
	}
	if generation == "" {
		generation = "0"
	}
	out := fmt.Sprintf(
		`<candidate component="%s" foundation="%s" generation="%s" id="%s" ip="%s" network="0" port="%s" priority="%s" protocol="%s" type="%s"`,
		component, foundation, generation, foundation+component, ip, port, priority, strings.ToLower(protocol), candType)
	if raddr != "" {
		out += fmt.Sprintf(` rel-addr="%s" rel-port="%s"`, raddr, rport)
	}
	if tcptype != "" {
		out += fmt.Sprintf(` tcptype="%s"`, tcptype)
	}
	return out + "/>"
}
