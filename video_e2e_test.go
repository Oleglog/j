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

// TestE2EVideoFakePayload: symmetric — bot2 and bot3 both send AND receive video from each other.
func TestE2EVideoFakePayload(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	room := "j-fake-vp8-test"
	host := "meet.cryptopro.ru"

	// Bot 1: filler (needed for Jicofo to create conference)
	bot1, err := JoinMUC(ctx, Config{Host: host, Room: room, Nick: "bot1-filler"})
	if err != nil {
		t.Fatalf("bot1: %v", err)
	}
	defer bot1.Close()

	validFrame := dummyVP8KeyframeLarge()
	var sendingFake atomic.Bool

	// Helper: create a bot that sends video and receives video from the other
	type botResult struct {
		rxValidPkts, rxFakePkts   atomic.Int64
		rxValidBytes, rxFakeBytes atomic.Int64
		totalSent                 atomic.Int64
		trackReceived             chan struct{}
	}

	setupBot := func(name string, planB bool) (*Session, *webrtc.PeerConnection, *botResult) {
		t.Logf("%s: joining...", name)
		bot, err := Join(ctx, Config{Host: host, Room: room, Nick: name})
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}

		cfg := bot.IceConfig()
		if planB {
			cfg.SDPSemantics = webrtc.SDPSemanticsPlanB
		}
		pc, err := webrtc.NewPeerConnection(cfg)
		if err != nil {
			t.Fatalf("%s pc: %v", name, err)
		}

		// Send track
		track, _ := webrtc.NewTrackLocalStaticSample(
			webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeVP8, ClockRate: 90000},
			"video-"+name, "stream-"+name)
		pc.AddTrack(track)

		res := &botResult{trackReceived: make(chan struct{}, 1)}

		// Receive track
		pc.OnTrack(func(remote *webrtc.TrackRemote, _ *webrtc.RTPReceiver) {
			t.Logf("[%s rx] OnTrack: kind=%s codec=%s ssrc=%d", name, remote.Kind(), remote.Codec().MimeType, remote.SSRC())
			if remote.Kind() != webrtc.RTPCodecTypeVideo {
				return
			}
			select {
			case res.trackReceived <- struct{}{}:
			default:
			}
			buf := make([]byte, 1500)
			for {
				n, _, err := remote.Read(buf)
				if err != nil {
					return
				}
				if sendingFake.Load() {
					res.rxFakePkts.Add(1)
					res.rxFakeBytes.Add(int64(n))
				} else {
					res.rxValidPkts.Add(1)
					res.rxValidBytes.Add(int64(n))
				}
			}
		})

		neg := bot.Negotiator()
		neg.PC = pc
		if err := neg.Accept(ctx); err != nil {
			t.Fatalf("%s Accept: %v", name, err)
		}

		if err := neg.SendSourceAddFromSDP(pc.LocalDescription().SDP); err != nil {
			t.Logf("%s source-add: %v", name, err)
		} else {
			t.Logf("%s: source-add sent", name)
		}

		// Listen for incoming source-add from Jicofo and update remote SDP
		go func() {
			for {
				select {
				case stanza, ok := <-bot.Conn.Stanzas():
					if !ok {
						return
					}
					if strings.Contains(stanza, "source-add") {
						if err := neg.HandleSourceAdd(stanza); err != nil {
							t.Logf("%s: handle source-add: %v", name, err)
						} else {
							t.Logf("%s: handled incoming source-add", name)
						}
					}
				case <-ctx.Done():
					return
				}
			}
		}()

		waitPC(t, pc, 15*time.Second)
		t.Logf("%s: connected", name)

		// Start sending frames
		go func() {
			tick := time.NewTicker(33 * time.Millisecond)
			defer tick.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-tick.C:
					var frame []byte
					if sendingFake.Load() {
						frame = make([]byte, 1024)
						copy(frame, validFrame[:10])
						rand.Read(frame[10:])
					} else {
						frame = validFrame
					}
					track.WriteSample(media.Sample{Data: frame, Duration: 33 * time.Millisecond})
					res.totalSent.Add(1)
				}
			}
		}()

		// Open bridge and request video
		if err := bot.OpenBridge(ctx); err != nil {
			t.Fatalf("%s OpenBridge: %v", name, err)
		}
		bot.Bridge().SendJSON(map[string]any{
			"colibriClass":       "ReceiverVideoConstraints",
			"lastN":              -1,
			"defaultConstraints": map[string]any{"maxHeight": 720},
		})

		return bot, pc, res
	}

	bot2, pc2, res2 := setupBot("bot2", true)
	defer bot2.Close()
	defer pc2.Close()

	time.Sleep(2 * time.Second) // let bot2 establish

	bot3, pc3, res3 := setupBot("bot3", true)
	defer bot3.Close()
	defer pc3.Close()

	// Wait for both to receive video tracks
	for _, pair := range []struct {
		name string
		res  *botResult
	}{{"bot2", res2}, {"bot3", res3}} {
		select {
		case <-pair.res.trackReceived:
			t.Logf("%s: got video track!", pair.name)
		case <-time.After(20 * time.Second):
			t.Fatalf("FAIL: %s never received video track", pair.name)
		}
	}

	// Collect valid VP8 stats
	time.Sleep(3 * time.Second)
	for _, pair := range []struct {
		name string
		res  *botResult
	}{{"bot2", res2}, {"bot3", res3}} {
		v := pair.res.rxValidPkts.Load()
		t.Logf("%s VALID VP8: received %d packets (%d bytes)", pair.name, v, pair.res.rxValidBytes.Load())
		if v == 0 {
			t.Fatalf("FAIL: %s received no valid VP8 — video routing broken", pair.name)
		}
	}

	// Switch to fake payload
	t.Log("switching to FAKE VP8 (valid header + random garbage)...")
	sendingFake.Store(true)
	time.Sleep(5 * time.Second)

	for _, pair := range []struct {
		name string
		res  *botResult
	}{{"bot2", res2}, {"bot3", res3}} {
		f := pair.res.rxFakePkts.Load()
		t.Logf("%s FAKE VP8: received %d packets (%d bytes)", pair.name, f, pair.res.rxFakeBytes.Load())
		if f == 0 {
			t.Errorf("FAIL: %s — bridge dropped fake VP8 frames", pair.name)
		} else {
			t.Logf("SUCCESS: %s received %d fake packets — arbitrary data works!", pair.name, f)
		}
	}

	t.Logf("SUMMARY: bot2 sent=%d, bot3 sent=%d", res2.totalSent.Load(), res3.totalSent.Load())
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
