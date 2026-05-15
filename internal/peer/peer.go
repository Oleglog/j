// Package peer bridges a pion *webrtc.PeerConnection with a Jicofo Jingle session.
// It is intentionally low-level: caller owns the PC and does whatever they want with
// tracks/data channels. peer just wires up SDP↔Jingle conversion and signalling.
package peer

import (
	"context"
	"fmt"
	"strings"
	"time"

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

	// Detect Plan B: if a single m=video section contains multiple SSRCs from
	// different sources, pion's UnifiedPlan will reject it. Detect and error
	// so caller can recreate PC with SDPSemanticsPlanB.
	if isPlanB(offerSDP) {
		// Try setting with current PC — if it fails, return a typed error
		err := n.PC.SetRemoteDescription(webrtc.SessionDescription{
			Type: webrtc.SDPTypeOffer,
			SDP:  offerSDP,
		})
		if err != nil && strings.Contains(err.Error(), "PlanB") {
			return fmt.Errorf("peer: remote SDP is Plan B — recreate PeerConnection with webrtc.SDPSemanticsPlanB: %w", err)
		}
		if err != nil {
			return fmt.Errorf("set remote desc: %w", err)
		}
	} else {
		if err := n.PC.SetRemoteDescription(webrtc.SessionDescription{
			Type: webrtc.SDPTypeOffer,
			SDP:  offerSDP,
		}); err != nil {
			return fmt.Errorf("set remote desc: %w", err)
		}
	}

	answer, err := n.PC.CreateAnswer(nil)
	if err != nil {
		return fmt.Errorf("create answer: %w", err)
	}
	if err := n.PC.SetLocalDescription(answer); err != nil {
		return fmt.Errorf("set local desc: %w", err)
	}

	// Wait for ICE gathering to complete (or timeout) so the session-accept
	// already contains all collected candidates. Late candidates after this
	// will be sent via trickle (transport-info).
	select {
	case <-webrtc.GatheringCompletePromise(n.PC):
	case <-time.After(3 * time.Second):
	case <-ctx.Done():
		return ctx.Err()
	}

	final := n.PC.LocalDescription().SDP
	jingleAccept := jingle.SDPToJingleAccept(final)

	if err := n.XMPP.SendSessionAccept(n.parsed.SID, n.parsed.Initiator, n.RoomJID, jingleAccept); err != nil {
		return err
	}

	// trickle ICE: any candidates discovered AFTER we sent session-accept
	// (e.g. late TURN allocations) get pushed via transport-info.
	n.PC.OnICECandidate(func(c *webrtc.ICECandidate) {
		if c == nil {
			return
		}
		js := c.ToJSON()
		raw := strings.TrimPrefix(js.Candidate, "candidate:")
		mediaName := "audio"
		if js.SDPMid != nil {
			mediaName = mediaNameForMid(*js.SDPMid)
		}
		_ = n.SendTransportInfo(mediaName, raw)
	})

	return nil
}

// mediaNameForMid resolves a mid like "0"/"1" or "audio"/"video" to "audio" or "video".
// Used for transport-info routing.
func mediaNameForMid(mid string) string {
	switch mid {
	case "0", "audio":
		return "audio"
	case "1", "video":
		return "video"
	case "2", "data":
		return "data"
	default:
		return mid
	}
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

// SendSourceAddFromSDP convenience: extracts <source>/<ssrc-group> elements per content
// from the given local SDP (typically pc.LocalDescription().SDP after AddTrack) and
// sends them via source-add to Jicofo.
func (n *Negotiator) SendSourceAddFromSDP(sdp string) error {
	if err := n.ensureParsed(); err != nil {
		return err
	}
	xmlBody := jingle.SDPSourcesXML(sdp)
	if xmlBody == "" {
		return fmt.Errorf("peer: no <source> elements found in SDP")
	}
	return n.SendSourceAdd(xmlBody)
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

// isPlanB detects Plan B SDP: multiple a=ssrc lines with different cname values
// in a single m=video section (Jicofo sends this when other participants have video).
func isPlanB(sdp string) bool {
	inVideo := false
	cnames := map[string]struct{}{}
	for _, line := range strings.Split(sdp, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "m=video") {
			inVideo = true
			cnames = map[string]struct{}{}
		} else if strings.HasPrefix(line, "m=") {
			if inVideo && len(cnames) > 1 {
				return true
			}
			inVideo = false
		}
		if inVideo && strings.HasPrefix(line, "a=ssrc:") && strings.Contains(line, "cname:") {
			parts := strings.SplitN(line, "cname:", 2)
			if len(parts) == 2 {
				cnames[strings.TrimSpace(parts[1])] = struct{}{}
			}
		}
	}
	return inVideo && len(cnames) > 1
}

// IsPlanBError returns true if the error from Accept indicates a Plan B SDP mismatch.
func IsPlanBError(err error) bool {
	return err != nil && strings.Contains(err.Error(), "Plan B")
}
