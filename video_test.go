package j

import (
	"testing"

	"github.com/pion/webrtc/v4"
	"github.com/zarazaex69/j/internal/jingle"
)

// sampleJingleWithVideo is a minimal Jingle session-initiate stanza containing
// audio + video + data contents (as Jicofo would send).
const sampleJingleWithVideo = `<iq type="set" from="myroom@conference.meet.cryptopro.ru/focus" to="user@meet.cryptopro.ru/abc123">
<jingle xmlns="urn:xmpp:jingle:1" action="session-initiate" initiator="myroom@conference.meet.cryptopro.ru/focus" sid="test-sid-123">
<group xmlns="urn:xmpp:jingle:apps:grouping:0" semantics="BUNDLE">
<content name="audio"/><content name="video"/><content name="data"/>
</group>
<content creator="initiator" name="audio" senders="both">
<description xmlns="urn:xmpp:jingle:apps:rtp:1" media="audio">
<payload-type id="111" name="opus" clockrate="48000" channels="2">
<parameter name="minptime" value="10"/>
<rtcp-fb xmlns="urn:xmpp:jingle:apps:rtp:rtcp-fb:0" type="transport-cc"/>
</payload-type>
<rtp-hdrext xmlns="urn:xmpp:jingle:apps:rtp:rtp-hdrext:0" id="1" uri="urn:ietf:params:rtp-hdrext:ssrc-audio-level"/>
<rtcp-mux/>
<source xmlns="urn:xmpp:jingle:apps:rtp:ssma:0" ssrc="11111111">
<parameter name="cname" value="audio-cname"/>
<parameter name="msid" value="stream0 audio0"/>
</source>
</description>
<transport xmlns="urn:xmpp:jingle:transports:ice-udp:1" ufrag="aaaa" pwd="bbbbbbbbbbbbbbbbbbbbbbbb">
<fingerprint xmlns="urn:xmpp:jingle:apps:dtls:0" hash="sha-256" setup="actpass">AA:BB:CC:DD:EE:FF:00:11:22:33:44:55:66:77:88:99:AA:BB:CC:DD:EE:FF:00:11:22:33:44:55:66:77:88:99</fingerprint>
<candidate component="1" foundation="1" generation="0" id="c1" ip="192.168.1.1" port="10000" priority="2130706431" protocol="udp" type="host"/>
</transport>
</content>
<content creator="initiator" name="video" senders="both">
<description xmlns="urn:xmpp:jingle:apps:rtp:1" media="video">
<payload-type id="100" name="VP8" clockrate="90000">
<rtcp-fb xmlns="urn:xmpp:jingle:apps:rtp:rtcp-fb:0" type="ccm" subtype="fir"/>
<rtcp-fb xmlns="urn:xmpp:jingle:apps:rtp:rtcp-fb:0" type="nack"/>
<rtcp-fb xmlns="urn:xmpp:jingle:apps:rtp:rtcp-fb:0" type="nack" subtype="pli"/>
<rtcp-fb xmlns="urn:xmpp:jingle:apps:rtp:rtcp-fb:0" type="transport-cc"/>
</payload-type>
<payload-type id="96" name="VP9" clockrate="90000">
<rtcp-fb xmlns="urn:xmpp:jingle:apps:rtp:rtcp-fb:0" type="ccm" subtype="fir"/>
<rtcp-fb xmlns="urn:xmpp:jingle:apps:rtp:rtcp-fb:0" type="nack"/>
<rtcp-fb xmlns="urn:xmpp:jingle:apps:rtp:rtcp-fb:0" type="nack" subtype="pli"/>
</payload-type>
<rtp-hdrext xmlns="urn:xmpp:jingle:apps:rtp:rtp-hdrext:0" id="3" uri="http://www.ietf.org/id/draft-holmer-rmcat-transport-wide-cc-extensions-01"/>
<rtcp-mux/>
<source xmlns="urn:xmpp:jingle:apps:rtp:ssma:0" ssrc="22222222">
<parameter name="cname" value="video-cname"/>
<parameter name="msid" value="stream0 video0"/>
</source>
<ssrc-group xmlns="urn:xmpp:jingle:apps:rtp:ssma:0" semantics="FID">
<source ssrc="22222222"/><source ssrc="33333333"/>
</ssrc-group>
</description>
<transport xmlns="urn:xmpp:jingle:transports:ice-udp:1" ufrag="aaaa" pwd="bbbbbbbbbbbbbbbbbbbbbbbb">
<fingerprint xmlns="urn:xmpp:jingle:apps:dtls:0" hash="sha-256" setup="actpass">AA:BB:CC:DD:EE:FF:00:11:22:33:44:55:66:77:88:99:AA:BB:CC:DD:EE:FF:00:11:22:33:44:55:66:77:88:99</fingerprint>
<candidate component="1" foundation="1" generation="0" id="c1" ip="192.168.1.1" port="10000" priority="2130706431" protocol="udp" type="host"/>
</transport>
</content>
<content creator="initiator" name="data" senders="both">
<description xmlns="urn:xmpp:jingle:transports:dtls-sctp:1"/>
<transport xmlns="urn:xmpp:jingle:transports:ice-udp:1" ufrag="aaaa" pwd="bbbbbbbbbbbbbbbbbbbbbbbb">
<fingerprint xmlns="urn:xmpp:jingle:apps:dtls:0" hash="sha-256" setup="actpass">AA:BB:CC:DD:EE:FF:00:11:22:33:44:55:66:77:88:99:AA:BB:CC:DD:EE:FF:00:11:22:33:44:55:66:77:88:99</fingerprint>
<sctpmap xmlns="urn:xmpp:jingle:transports:dtls-sctp:1" number="5000" protocol="webrtc-datachannel" streams="1024"/>
</transport>
</content>
</jingle>
</iq>`

func TestParseJingleVideoContent(t *testing.T) {
	parsed := jingle.Parse(sampleJingleWithVideo)
	if parsed.SID != "test-sid-123" {
		t.Fatalf("SID = %q, want test-sid-123", parsed.SID)
	}
	if len(parsed.VideoSources) == 0 {
		t.Fatal("no video sources parsed from jingle stanza")
	}
	if parsed.VideoSources[0].SSRC != "22222222" {
		t.Errorf("video SSRC = %q, want 22222222", parsed.VideoSources[0].SSRC)
	}
	if len(parsed.AudioSources) == 0 {
		t.Fatal("no audio sources parsed")
	}
}

func TestJingleToSDPContainsVideo(t *testing.T) {
	parsed := jingle.Parse(sampleJingleWithVideo)
	sdp := parsed.SDP
	if sdp == "" {
		t.Fatal("SDP is empty")
	}
	// Must contain video m-line
	if !containsLine(sdp, "m=video") {
		t.Error("SDP missing m=video line")
	}
	// Must contain VP8 codec
	if !containsLine(sdp, "VP8/90000") {
		t.Error("SDP missing VP8 rtpmap")
	}
	// Must contain VP9 codec
	if !containsLine(sdp, "VP9/90000") {
		t.Error("SDP missing VP9 rtpmap")
	}
	// Must have BUNDLE with video
	if !containsLine(sdp, "a=group:BUNDLE") {
		t.Error("SDP missing BUNDLE group")
	}
	// Must have video SSRC
	if !containsLine(sdp, "a=ssrc:22222222") {
		t.Error("SDP missing video SSRC 22222222")
	}
	// Must have ssrc-group FID for simulcast/RTX
	if !containsLine(sdp, "a=ssrc-group:FID") {
		t.Error("SDP missing ssrc-group:FID")
	}
}

func TestSDPToJingleAcceptPreservesVideo(t *testing.T) {
	// Simulate a local SDP answer with video
	localSDP := `v=0
o=- 0 0 IN IP4 127.0.0.1
s=-
t=0 0
a=group:BUNDLE audio video
a=msid-semantic: WMS *
m=audio 9 UDP/TLS/RTP/SAVPF 111
c=IN IP4 0.0.0.0
a=mid:audio
a=recvonly
a=rtcp-mux
a=ice-ufrag:xxxx
a=ice-pwd:yyyyyyyyyyyyyyyyyyyyyyyy
a=fingerprint:sha-256 AA:BB:CC:DD:EE:FF:00:11:22:33:44:55:66:77:88:99:AA:BB:CC:DD:EE:FF:00:11:22:33:44:55:66:77:88:99
a=setup:active
a=rtpmap:111 opus/48000/2
m=video 9 UDP/TLS/RTP/SAVPF 100
c=IN IP4 0.0.0.0
a=mid:video
a=sendonly
a=rtcp-mux
a=ice-ufrag:xxxx
a=ice-pwd:yyyyyyyyyyyyyyyyyyyyyyyy
a=fingerprint:sha-256 AA:BB:CC:DD:EE:FF:00:11:22:33:44:55:66:77:88:99:AA:BB:CC:DD:EE:FF:00:11:22:33:44:55:66:77:88:99
a=setup:active
a=rtpmap:100 VP8/90000
a=ssrc:99999999 cname:localvideo
a=ssrc:99999999 msid:jstream jvideo
`
	xml := jingle.SDPToJingleAccept(localSDP)
	if xml == "" {
		t.Fatal("SDPToJingleAccept returned empty")
	}
	// Must contain video content
	if !containsStr(xml, `name="video"`) {
		t.Error("Jingle accept XML missing video content")
	}
	// Must contain VP8 payload
	if !containsStr(xml, `name="VP8"`) {
		t.Error("Jingle accept XML missing VP8 payload-type")
	}
	// Must have senders="responder" (from sendonly)
	if !containsStr(xml, `senders="responder"`) {
		t.Error("Jingle accept XML missing senders=responder for video")
	}
	// Must contain our local SSRC
	if !containsStr(xml, `ssrc="99999999"`) {
		t.Error("Jingle accept XML missing local video SSRC")
	}
}

func TestSDPSourcesXMLExtractsVideo(t *testing.T) {
	sdp := `v=0
o=- 0 0 IN IP4 127.0.0.1
s=-
t=0 0
a=group:BUNDLE audio video
m=audio 9 UDP/TLS/RTP/SAVPF 111
c=IN IP4 0.0.0.0
a=mid:audio
a=rtpmap:111 opus/48000/2
a=ssrc:11111111 cname:a
m=video 9 UDP/TLS/RTP/SAVPF 100
c=IN IP4 0.0.0.0
a=mid:video
a=rtpmap:100 VP8/90000
a=ssrc:22222222 cname:v
a=ssrc:22222222 msid:stream video0
`
	xml := jingle.SDPSourcesXML(sdp)
	if !containsStr(xml, `name="video"`) {
		t.Error("SDPSourcesXML missing video content")
	}
	if !containsStr(xml, `ssrc="22222222"`) {
		t.Error("SDPSourcesXML missing video SSRC")
	}
}

func TestCreateVP8Track(t *testing.T) {
	track, err := webrtc.NewTrackLocalStaticSample(
		webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeVP8, ClockRate: 90000},
		"jvideo", "jstream")
	if err != nil {
		t.Fatalf("failed to create VP8 track: %v", err)
	}
	if track.Codec().MimeType != webrtc.MimeTypeVP8 {
		t.Errorf("track codec = %q, want %q", track.Codec().MimeType, webrtc.MimeTypeVP8)
	}
	if track.ID() != "jvideo" {
		t.Errorf("track ID = %q, want jvideo", track.ID())
	}
}

func TestPeerConnectionCanAddVideoTrack(t *testing.T) {
	pc, err := webrtc.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Fatalf("new pc: %v", err)
	}
	defer func() { _ = pc.Close() }()

	track, _ := webrtc.NewTrackLocalStaticSample(
		webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeVP8, ClockRate: 90000},
		"jvideo", "jstream")

	sender, err := pc.AddTrack(track)
	if err != nil {
		t.Fatalf("AddTrack(VP8): %v", err)
	}
	if sender == nil {
		t.Fatal("sender is nil after AddTrack")
	}

	// Verify the transceiver was created with sendrecv direction
	transceivers := pc.GetTransceivers()
	found := false
	for _, tr := range transceivers {
		if tr.Kind() == webrtc.RTPCodecTypeVideo {
			found = true
			break
		}
	}
	if !found {
		t.Error("no video transceiver found after AddTrack")
	}
}

func TestSetRemoteDescriptionWithVideoOffer(t *testing.T) {
	parsed := jingle.Parse(sampleJingleWithVideo)
	sdp := parsed.SDP
	if sdp == "" {
		t.Fatal("failed to generate SDP from jingle")
	}

	pc, err := webrtc.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Fatalf("new pc: %v", err)
	}
	defer func() { _ = pc.Close() }()

	// Add recvonly video transceiver (as our client does)
	_, err = pc.AddTransceiverFromKind(webrtc.RTPCodecTypeVideo,
		webrtc.RTPTransceiverInit{Direction: webrtc.RTPTransceiverDirectionRecvonly})
	if err != nil {
		t.Fatalf("add video transceiver: %v", err)
	}

	err = pc.SetRemoteDescription(webrtc.SessionDescription{
		Type: webrtc.SDPTypeOffer,
		SDP:  sdp,
	})
	if err != nil {
		t.Fatalf("SetRemoteDescription with video offer failed: %v", err)
	}

	answer, err := pc.CreateAnswer(nil)
	if err != nil {
		t.Fatalf("CreateAnswer failed: %v", err)
	}
	if !containsLine(answer.SDP, "m=video") {
		t.Error("answer SDP missing m=video")
	}
}

func TestSetRemoteDescriptionWithSendVideo(t *testing.T) {
	parsed := jingle.Parse(sampleJingleWithVideo)
	sdp := parsed.SDP

	pc, err := webrtc.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Fatalf("new pc: %v", err)
	}
	defer func() { _ = pc.Close() }()

	// Add sendonly video track (as -send-video does)
	track, _ := webrtc.NewTrackLocalStaticSample(
		webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeVP8, ClockRate: 90000},
		"jvideo", "jstream")
	_, err = pc.AddTrack(track)
	if err != nil {
		t.Fatalf("AddTrack: %v", err)
	}

	err = pc.SetRemoteDescription(webrtc.SessionDescription{
		Type: webrtc.SDPTypeOffer,
		SDP:  sdp,
	})
	if err != nil {
		t.Fatalf("SetRemoteDescription with video (sendonly track) failed: %v", err)
	}

	answer, err := pc.CreateAnswer(nil)
	if err != nil {
		t.Fatalf("CreateAnswer failed: %v", err)
	}
	if !containsLine(answer.SDP, "m=video") {
		t.Error("answer SDP missing m=video")
	}
	// The answer should contain our SSRC
	if !containsLine(answer.SDP, "a=ssrc:") {
		t.Error("answer SDP missing local SSRC for video track")
	}
}

func containsLine(s, substr string) bool {
	for _, line := range splitLines(s) {
		if containsStr(line, substr) {
			return true
		}
	}
	return false
}

func splitLines(s string) []string {
	return splitByNewline(s)
}

func splitByNewline(s string) []string {
	var result []string
	start := 0
	for i := 0; i < len(s); i++ {
		if s[i] == '\n' {
			result = append(result, s[start:i])
			start = i + 1
		}
	}
	if start < len(s) {
		result = append(result, s[start:])
	}
	return result
}

func containsStr(s, sub string) bool {
	return len(s) >= len(sub) && (s == sub || len(sub) == 0 || findStr(s, sub))
}

func findStr(s, sub string) bool {
	for i := 0; i <= len(s)-len(sub); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
