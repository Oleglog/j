//go:build e2e

package j

import (
	"context"
	"crypto/rand"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pion/webrtc/v4"
	"github.com/pion/webrtc/v4/pkg/media"
)

// TestE2EVideoFakePayload sends arbitrary data disguised as VP8 frames.
// VP8 keyframe header (first few bytes) is valid, the rest is random garbage.
// Checks if the bridge routes it and the other side receives RTP packets.
func TestE2EVideoFakePayload(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	room := "j-video-fake-payload"
	host := "meet.cryptopro.ru"

	// Bot 1: sender — sends fake VP8 with arbitrary data inside
	bot1, err := Join(ctx, Config{Host: host, Room: room, Nick: "bot1-sender"})
	if err != nil {
		t.Fatalf("bot1 Join: %v", err)
	}
	defer bot1.Close()

	// Bot 2: receiver
	bot2, err := Join(ctx, Config{Host: host, Room: room, Nick: "bot2-receiver"})
	if err != nil {
		t.Fatalf("bot2 Join: %v", err)
	}
	defer bot2.Close()

	// --- Bot 1: send fake VP8 ---
	pc1, err := webrtc.NewPeerConnection(bot1.IceConfig())
	if err != nil {
		t.Fatalf("pc1: %v", err)
	}
	defer pc1.Close()

	sendTrack, _ := webrtc.NewTrackLocalStaticSample(
		webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeVP8, ClockRate: 90000},
		"fakevideo", "fakestream")
	pc1.AddTrack(sendTrack)

	neg1 := bot1.Negotiator()
	neg1.PC = pc1
	if err := neg1.Accept(ctx); err != nil {
		t.Fatalf("bot1 Accept: %v", err)
	}

	// --- Bot 2: receive ---
	pc2, err := webrtc.NewPeerConnection(bot2.IceConfig())
	if err != nil {
		t.Fatalf("pc2: %v", err)
	}
	defer pc2.Close()

	pc2.AddTransceiverFromKind(webrtc.RTPCodecTypeAudio, webrtc.RTPTransceiverInit{Direction: webrtc.RTPTransceiverDirectionRecvonly})
	pc2.AddTransceiverFromKind(webrtc.RTPCodecTypeVideo, webrtc.RTPTransceiverInit{Direction: webrtc.RTPTransceiverDirectionRecvonly})

	var rxPackets atomic.Int64
	var rxBytes atomic.Int64
	pc2.OnTrack(func(track *webrtc.TrackRemote, _ *webrtc.RTPReceiver) {
		t.Logf("[rx] track kind=%s codec=%s ssrc=%d", track.Kind(), track.Codec().MimeType, track.SSRC())
		if track.Kind() == webrtc.RTPCodecTypeVideo {
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

	neg2 := bot2.Negotiator()
	neg2.PC = pc2
	if err := neg2.Accept(ctx); err != nil {
		t.Fatalf("bot2 Accept: %v", err)
	}

	// Wait for both PCs to connect
	waitConnected(t, pc1, 15*time.Second)
	waitConnected(t, pc2, 15*time.Second)

	// Send fake VP8: valid header prefix + random garbage payload
	// VP8 keyframe starts with: 0x10 (partition 0, keyframe bit)
	// then 3 bytes frame tag, then we stuff whatever we want
	t.Log("sending fake VP8 frames with random data payload...")
	vp8Header := []byte{0x10, 0x02, 0x00, 0x9d, 0x01, 0x2a, 0x40, 0x00, 0x40, 0x00} // 64x64 keyframe start

	var totalSent int64
	go func() {
		tick := time.NewTicker(33 * time.Millisecond) // 30fps
		defer tick.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
				// 1KB frame: valid VP8 header + random garbage
				payload := make([]byte, 1024)
				copy(payload, vp8Header)
				rand.Read(payload[len(vp8Header):]) // random data after header
				_ = sendTrack.WriteSample(media.Sample{Data: payload, Duration: 33 * time.Millisecond})
				totalSent++
			}
		}
	}()

	// Let it run for 5 seconds
	time.Sleep(5 * time.Second)

	rx := rxPackets.Load()
	rxB := rxBytes.Load()
	t.Logf("sent ~%d frames, received %d RTP packets (%d bytes)", totalSent, rx, rxB)

	if rx == 0 {
		t.Error("FAIL: no video RTP packets received — bridge dropped fake VP8 frames")
	} else {
		t.Logf("SUCCESS: bridge routed %d packets with fake payload data!", rx)
		// Check we got meaningful amount of data through
		if rxB > 1000 {
			t.Logf("confirmed: arbitrary data (%d bytes) passed through video channel", rxB)
		}
	}

	neg1.Terminate("success")
	neg2.Terminate("success")
}

// TestE2EVideoLargePayload pushes bigger fake frames to see limits
func TestE2EVideoLargePayload(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	room := "j-video-large-payload"
	host := "meet.cryptopro.ru"

	bot1, err := Join(ctx, Config{Host: host, Room: room, Nick: "bot1-big"})
	if err != nil {
		t.Fatalf("bot1: %v", err)
	}
	defer bot1.Close()

	bot2, err := Join(ctx, Config{Host: host, Room: room, Nick: "bot2-big"})
	if err != nil {
		t.Fatalf("bot2: %v", err)
	}
	defer bot2.Close()

	pc1, _ := webrtc.NewPeerConnection(bot1.IceConfig())
	defer pc1.Close()
	sendTrack, _ := webrtc.NewTrackLocalStaticSample(
		webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeVP8, ClockRate: 90000},
		"bigvideo", "bigstream")
	pc1.AddTrack(sendTrack)
	neg1 := bot1.Negotiator()
	neg1.PC = pc1
	neg1.Accept(ctx)

	pc2, _ := webrtc.NewPeerConnection(bot2.IceConfig())
	defer pc2.Close()
	pc2.AddTransceiverFromKind(webrtc.RTPCodecTypeAudio, webrtc.RTPTransceiverInit{Direction: webrtc.RTPTransceiverDirectionRecvonly})
	pc2.AddTransceiverFromKind(webrtc.RTPCodecTypeVideo, webrtc.RTPTransceiverInit{Direction: webrtc.RTPTransceiverDirectionRecvonly})

	var rxBytes atomic.Int64
	pc2.OnTrack(func(track *webrtc.TrackRemote, _ *webrtc.RTPReceiver) {
		if track.Kind() == webrtc.RTPCodecTypeVideo {
			buf := make([]byte, 65535)
			for {
				n, _, err := track.Read(buf)
				if err != nil {
					return
				}
				rxBytes.Add(int64(n))
			}
		}
	})
	neg2 := bot2.Negotiator()
	neg2.PC = pc2
	neg2.Accept(ctx)

	waitConnected(t, pc1, 15*time.Second)
	waitConnected(t, pc2, 15*time.Second)

	// Send large frames: 50KB each (will be fragmented into RTP packets)
	sizes := []int{4096, 16384, 65000}
	for _, size := range sizes {
		rxBytes.Store(0)
		vp8Header := []byte{0x10, 0x02, 0x00, 0x9d, 0x01, 0x2a, 0x40, 0x00, 0x40, 0x00}
		payload := make([]byte, size)
		copy(payload, vp8Header)
		rand.Read(payload[len(vp8Header):])

		for i := 0; i < 30; i++ { // 1 second worth
			sendTrack.WriteSample(media.Sample{Data: payload, Duration: 33 * time.Millisecond})
			time.Sleep(33 * time.Millisecond)
		}
		time.Sleep(500 * time.Millisecond)
		got := rxBytes.Load()
		t.Logf("frame_size=%d: received %d bytes (%.1f%%)", size, got, float64(got)/float64(int64(size)*30)*100)
		if got == 0 {
			t.Errorf("frame_size=%d: nothing received", size)
		}
	}

	neg1.Terminate("success")
	neg2.Terminate("success")
}

func waitConnected(t *testing.T, pc *webrtc.PeerConnection, timeout time.Duration) {
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
		t.Logf("warning: PC not connected after %v (state=%s)", timeout, pc.ConnectionState())
	}
}

// --- Original tests below ---

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

	connectedCh := make(chan struct{}, 1)
	pc.OnConnectionStateChange(func(s webrtc.PeerConnectionState) {
		if s == webrtc.PeerConnectionStateConnected {
			select {
			case connectedCh <- struct{}{}:
			default:
			}
		}
	})

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

	select {
	case <-connectedCh:
		t.Log("SUCCESS: PeerConnection connected — video works!")
	case <-time.After(20 * time.Second):
		t.Log("ICE timeout but SDP negotiation succeeded")
	}
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

func init() {
	// suppress pion's verbose logging in tests
	fmt.Print("")
}
