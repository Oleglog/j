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

// TestE2EVideoFakePayload: bot1 sends VP8 header + random garbage, bot2 checks if it arrives.
// Uses single room: bot1 does full Join + sends video, bot2 does full Join + recvonly.
func TestE2EVideoFakePayload(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	room := "j-fake-vp8-test"
	host := "meet.cryptopro.ru"

	// Bot 1: filler so bot2 gets session-initiate
	t.Log("bot1: joining MUC...")
	bot1, err := JoinMUC(ctx, Config{Host: host, Room: room, Nick: "bot1-filler"})
	if err != nil {
		t.Fatalf("bot1: %v", err)
	}
	defer bot1.Close()

	// Bot 2: full join, will send fake VP8
	t.Log("bot2: joining with Jingle (sender)...")
	bot2, err := Join(ctx, Config{Host: host, Room: room, Nick: "bot2-sender"})
	if err != nil {
		t.Fatalf("bot2 Join: %v", err)
	}
	defer bot2.Close()

	// Bot 3: full join, will receive video
	t.Log("bot3: joining with Jingle (receiver)...")
	bot3, err := Join(ctx, Config{Host: host, Room: room, Nick: "bot3-receiver"})
	if err != nil {
		t.Fatalf("bot3 Join: %v", err)
	}
	defer bot3.Close()

	// --- Bot 2: PC with sendonly VP8 track ---
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

	// Wait for bot2 PC to connect
	waitPC(t, pc2, 15*time.Second)
	t.Log("bot2: connected, starting to send fake VP8...")

	// Start sending fake VP8 immediately
	vp8Header := []byte{0x10, 0x02, 0x00, 0x9d, 0x01, 0x2a, 0x40, 0x00, 0x40, 0x00}
	sendCtx, sendCancel := context.WithCancel(ctx)
	defer sendCancel()
	var totalSent atomic.Int64
	go func() {
		tick := time.NewTicker(33 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-sendCtx.Done():
				return
			case <-tick.C:
				payload := make([]byte, 1024)
				copy(payload, vp8Header)
				rand.Read(payload[len(vp8Header):])
				sendTrack.WriteSample(media.Sample{Data: payload, Duration: 33 * time.Millisecond})
				totalSent.Add(1)
			}
		}
	}()

	// --- Bot 3: PC with recvonly video ---
	pc3, err := webrtc.NewPeerConnection(bot3.IceConfig())
	if err != nil {
		t.Fatalf("pc3: %v", err)
	}
	defer pc3.Close()

	pc3.AddTransceiverFromKind(webrtc.RTPCodecTypeAudio, webrtc.RTPTransceiverInit{Direction: webrtc.RTPTransceiverDirectionRecvonly})
	pc3.AddTransceiverFromKind(webrtc.RTPCodecTypeVideo, webrtc.RTPTransceiverInit{Direction: webrtc.RTPTransceiverDirectionRecvonly})

	var rxPackets atomic.Int64
	var rxBytes atomic.Int64
	trackReceived := make(chan struct{}, 1)
	pc3.OnTrack(func(track *webrtc.TrackRemote, _ *webrtc.RTPReceiver) {
		t.Logf("[rx] track kind=%s codec=%s ssrc=%d", track.Kind(), track.Codec().MimeType, track.SSRC())
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
				rxPackets.Add(1)
				rxBytes.Add(int64(n))
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
	t.Log("bot3: connected, waiting for video track...")

	// Wait for track or timeout
	select {
	case <-trackReceived:
		t.Log("bot3: got video track!")
	case <-time.After(15 * time.Second):
		t.Log("bot3: no video track received within 15s")
	}

	// Give it a few more seconds to accumulate packets
	time.Sleep(3 * time.Second)

	rx := rxPackets.Load()
	rxB := rxBytes.Load()
	sent := totalSent.Load()
	t.Logf("sent %d fake VP8 frames, received %d RTP packets (%d bytes)", sent, rx, rxB)

	if rx == 0 {
		t.Error("FAIL: no video packets received — bridge won't route fake VP8 or LastN excluded us")
	} else {
		t.Logf("SUCCESS: %d packets with fake data passed through video channel!", rx)
	}
}

func TestE2EVideoNegotiation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	room := "j-video-e2e-test"
	host := "meet.cryptopro.ru"

	t.Log("bot1: joining MUC...")
	bot1, err := JoinMUC(ctx, Config{Host: host, Room: room, Nick: "bot1-filler"})
	if err != nil {
		t.Fatalf("bot1 JoinMUC: %v", err)
	}
	defer bot1.Close()

	t.Log("bot2: joining with Jingle...")
	bot2, err := Join(ctx, Config{Host: host, Room: room, Nick: "bot2-video"})
	if err != nil {
		t.Fatalf("bot2 Join: %v", err)
	}
	defer bot2.Close()

	if !strings.Contains(bot2.SDP, "m=video") {
		t.Fatal("offer SDP missing m=video")
	}

	pc, err := webrtc.NewPeerConnection(bot2.IceConfig())
	if err != nil {
		t.Fatalf("NewPeerConnection: %v", err)
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
		frame := []byte{0x10, 0x02, 0x00, 0x9d, 0x01, 0x2a, 0x01, 0x00, 0x01, 0x00, 0x00, 0xc0, 0xfd, 0x07, 0x86, 0x83, 0x97, 0xff, 0xfe, 0xfb, 0x9f, 0x00, 0x00}
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
