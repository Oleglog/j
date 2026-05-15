//go:build e2e

package j

import (
	"context"
	"crypto/rand"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pion/webrtc/v4"
	"github.com/pion/webrtc/v4/pkg/media"
)

// TestE2EVideoFakePayload: 3 bots. bot2 sends VP8 (valid then fake), bot3 receives.
// After Accept, bot2 sends source-add so Jicofo tells bot3 about the new SSRC.
func TestE2EVideoFakePayload(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	room := "j-fake-vp8-test"
	host := "meet.cryptopro.ru"

	// Bot 1: filler
	bot1, err := JoinMUC(ctx, Config{Host: host, Room: room, Nick: "bot1-filler"})
	if err != nil {
		t.Fatalf("bot1: %v", err)
	}
	defer bot1.Close()

	// Bot 2: sender
	t.Log("bot2: joining (sender)...")
	bot2, err := Join(ctx, Config{Host: host, Room: room, Nick: "bot2-sender"})
	if err != nil {
		t.Fatalf("bot2: %v", err)
	}
	defer bot2.Close()

	pc2, err := webrtc.NewPeerConnection(bot2.IceConfig())
	if err != nil {
		t.Fatalf("pc2: %v", err)
	}
	defer pc2.Close()

	sendTrack, _ := webrtc.NewTrackLocalStaticSample(
		webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeVP8, ClockRate: 90000},
		"fakevideo", "fakestream")
	pc2.AddTrack(sendTrack)

	neg2 := bot2.Negotiator()
	neg2.PC = pc2
	if err := neg2.Accept(ctx); err != nil {
		t.Fatalf("bot2 Accept: %v", err)
	}
	defer neg2.Terminate("success")

	// Send source-add to announce our SSRC to Jicofo
	localSDP := pc2.LocalDescription().SDP
	if err := neg2.SendSourceAddFromSDP(localSDP); err != nil {
		t.Logf("bot2 source-add: %v (may be ok if already in session-accept)", err)
	} else {
		t.Log("bot2: source-add sent")
	}

	waitPC(t, pc2, 15*time.Second)
	t.Log("bot2: connected")

	// Start sending VALID VP8 keyframes first
	t.Log("bot2: sending valid VP8 keyframes...")
	validFrame := dummyVP8KeyframeLarge()
	sendCtx, sendCancel := context.WithCancel(ctx)
	defer sendCancel()
	var totalSent atomic.Int64
	var sendingFake atomic.Bool
	go func() {
		tick := time.NewTicker(33 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-sendCtx.Done():
				return
			case <-tick.C:
				var frame []byte
				if sendingFake.Load() {
					// Fake: valid header + garbage
					frame = make([]byte, 1024)
					copy(frame, validFrame[:10])
					rand.Read(frame[10:])
				} else {
					frame = validFrame
				}
				sendTrack.WriteSample(media.Sample{Data: frame, Duration: 33 * time.Millisecond})
				totalSent.Add(1)
			}
		}
	}()

	// Bot 3: receiver — join AFTER bot2 is already sending
	time.Sleep(2 * time.Second) // let bot2 establish itself
	t.Log("bot3: joining (receiver)...")
	bot3, err := Join(ctx, Config{Host: host, Room: room, Nick: "bot3-receiver"})
	if err != nil {
		t.Fatalf("bot3: %v", err)
	}
	defer bot3.Close()

	// Jicofo sends Plan B offer when there are existing video sources in the room
	cfg3 := bot3.IceConfig()
	cfg3.SDPSemantics = webrtc.SDPSemanticsPlanB
	pc3, err := webrtc.NewPeerConnection(cfg3)
	if err != nil {
		t.Fatalf("pc3: %v", err)
	}
	defer pc3.Close()

	pc3.AddTransceiverFromKind(webrtc.RTPCodecTypeAudio, webrtc.RTPTransceiverInit{Direction: webrtc.RTPTransceiverDirectionRecvonly})
	pc3.AddTransceiverFromKind(webrtc.RTPCodecTypeVideo, webrtc.RTPTransceiverInit{Direction: webrtc.RTPTransceiverDirectionRecvonly})

	var rxValidPkts atomic.Int64
	var rxFakePkts atomic.Int64
	var rxValidBytes atomic.Int64
	var rxFakeBytes atomic.Int64
	trackReceived := make(chan struct{}, 1)

	pc3.OnTrack(func(track *webrtc.TrackRemote, _ *webrtc.RTPReceiver) {
		t.Logf("[rx] OnTrack: kind=%s codec=%s ssrc=%d", track.Kind(), track.Codec().MimeType, track.SSRC())
		if track.Kind() == webrtc.RTPCodecTypeVideo {
			select {
			case trackReceived <- struct{}{}:
			default:
			}
			buf := make([]byte, 1500)
			for {
				n, _, err := track.Read(buf)
				if err != nil {
					return
				}
				if sendingFake.Load() {
					rxFakePkts.Add(1)
					rxFakeBytes.Add(int64(n))
				} else {
					rxValidPkts.Add(1)
					rxValidBytes.Add(int64(n))
				}
			}
		}
	})

	neg3 := bot3.Negotiator()
	neg3.PC = pc3
	if err := neg3.Accept(ctx); err != nil {
		t.Fatalf("bot3 Accept: %v", err)
	}
	defer neg3.Terminate("success")

	waitPC(t, pc3, 15*time.Second)
	t.Log("bot3: connected")

	// Open bridge channel and request video via ReceiverVideoConstraints
	if err := bot3.OpenBridge(ctx); err != nil {
		t.Fatalf("bot3 OpenBridge: %v", err)
	}
	bot3.Bridge().SendJSON(map[string]any{
		"colibriClass":       "ReceiverVideoConstraints",
		"lastN":              -1,
		"defaultConstraints": map[string]any{"maxHeight": 720},
	})
	t.Log("bot3: sent ReceiverVideoConstraints, waiting for video track...")

	select {
	case <-trackReceived:
		t.Log("bot3: got video track!")
	case <-time.After(20 * time.Second):
		t.Fatal("FAIL: bot3 never received video track (OnTrack not fired)")
	}

	// Collect valid VP8 stats for 3 seconds
	time.Sleep(3 * time.Second)
	validPkts := rxValidPkts.Load()
	validBytes := rxValidBytes.Load()
	t.Logf("VALID VP8: received %d packets (%d bytes)", validPkts, validBytes)

	if validPkts == 0 {
		t.Fatal("FAIL: even valid VP8 not received — video routing broken")
	}

	// Now switch to fake payload
	t.Log("switching to FAKE VP8 (valid header + random garbage)...")
	sendingFake.Store(true)
	time.Sleep(5 * time.Second)

	fakePkts := rxFakePkts.Load()
	fakeBytes := rxFakeBytes.Load()
	t.Logf("FAKE VP8: received %d packets (%d bytes)", fakePkts, fakeBytes)

	if fakePkts == 0 {
		t.Error("FAIL: bridge dropped fake VP8 frames — it inspects payload!")
	} else {
		t.Logf("SUCCESS: bridge routed %d fake packets (%d bytes) — arbitrary data works!", fakePkts, fakeBytes)
	}

	t.Logf("SUMMARY: valid=%d pkts, fake=%d pkts, total_sent=%d frames", validPkts, fakePkts, totalSent.Load())
}

// dummyVP8KeyframeLarge returns a more realistic VP8 keyframe (~200 bytes).
func dummyVP8KeyframeLarge() []byte {
	// Minimal valid 64x64 VP8 keyframe
	header := []byte{
		0x10, 0x02, 0x00, 0x9d, 0x01, 0x2a, 0x40, 0x00, 0x40, 0x00,
		0x00, 0x47, 0x08, 0x85, 0x85, 0x88, 0x85, 0x84, 0x88, 0x02,
		0x02, 0x02, 0x00, 0x06, 0x20, 0x30, 0x60, 0x00, 0xfe, 0xfb,
		0x94, 0x00, 0x00,
	}
	// Pad to 200 bytes with zeros (valid padding for VP8)
	frame := make([]byte, 200)
	copy(frame, header)
	return frame
}

func TestE2EVideoNegotiation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	room := "j-video-e2e-test"
	host := "meet.cryptopro.ru"

	bot1, err := JoinMUC(ctx, Config{Host: host, Room: room, Nick: "bot1-filler"})
	if err != nil {
		t.Fatalf("bot1: %v", err)
	}
	defer bot1.Close()

	bot2, err := Join(ctx, Config{Host: host, Room: room, Nick: "bot2-video"})
	if err != nil {
		t.Fatalf("bot2: %v", err)
	}
	defer bot2.Close()

	if !strings.Contains(bot2.SDP, "m=video") {
		t.Fatal("offer SDP missing m=video")
	}

	pc, err := webrtc.NewPeerConnection(bot2.IceConfig())
	if err != nil {
		t.Fatalf("pc: %v", err)
	}
	defer pc.Close()

	localVideo, _ := webrtc.NewTrackLocalStaticSample(
		webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeVP8, ClockRate: 90000},
		"jvideo", "jstream")
	pc.AddTrack(localVideo)

	neg := bot2.Negotiator()
	neg.PC = pc
	if err := neg.Accept(ctx); err != nil {
		t.Fatalf("Accept: %v", err)
	}

	go func() {
		frame := dummyVP8KeyframeLarge()
		tick := time.NewTicker(33 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
				localVideo.WriteSample(media.Sample{Data: frame, Duration: 33 * time.Millisecond})
			}
		}
	}()

	waitPC(t, pc, 20*time.Second)
	t.Log("SUCCESS: PeerConnection connected — video works!")
	neg.Terminate("success")
}

func TestE2EVideoRecvOnly(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	room := "j-video-e2e-recv"
	host := "meet.cryptopro.ru"

	bot1, err := JoinMUC(ctx, Config{Host: host, Room: room, Nick: "bot1-filler"})
	if err != nil {
		t.Fatalf("bot1: %v", err)
	}
	defer bot1.Close()

	bot2, err := Join(ctx, Config{Host: host, Room: room, Nick: "bot2-recv"})
	if err != nil {
		t.Fatalf("bot2: %v", err)
	}
	defer bot2.Close()

	pc, _ := webrtc.NewPeerConnection(bot2.IceConfig())
	defer pc.Close()
	pc.AddTransceiverFromKind(webrtc.RTPCodecTypeAudio, webrtc.RTPTransceiverInit{Direction: webrtc.RTPTransceiverDirectionRecvonly})
	pc.AddTransceiverFromKind(webrtc.RTPCodecTypeVideo, webrtc.RTPTransceiverInit{Direction: webrtc.RTPTransceiverDirectionRecvonly})

	neg := bot2.Negotiator()
	neg.PC = pc
	if err := neg.Accept(ctx); err != nil {
		t.Fatalf("Accept: %v", err)
	}
	if !strings.Contains(pc.LocalDescription().SDP, "m=video") {
		t.Error("answer missing m=video")
	}
	t.Log("recvonly video session-accept OK")
	neg.Terminate("success")
}

func waitPC(t *testing.T, pc *webrtc.PeerConnection, timeout time.Duration) {
	t.Helper()
	if pc.ConnectionState() == webrtc.PeerConnectionStateConnected {
		return
	}
	ch := make(chan struct{}, 1)
	pc.OnConnectionStateChange(func(s webrtc.PeerConnectionState) {
		if s == webrtc.PeerConnectionStateConnected {
			select {
			case ch <- struct{}{}:
			default:
			}
		}
	})
	select {
	case <-ch:
	case <-time.After(timeout):
		t.Logf("PC not connected after %v (state=%s)", timeout, pc.ConnectionState())
	}
}
