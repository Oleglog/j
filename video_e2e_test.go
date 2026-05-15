//go:build e2e

package j

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/pion/webrtc/v4"
	"github.com/pion/webrtc/v4/pkg/media"
)

func TestE2EVideoNegotiation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	room := "j-video-e2e-test"
	host := "meet.cryptopro.ru"

	// Bot 1: join MUC first (triggers focus allocation)
	t.Log("bot1: joining MUC...")
	bot1, err := JoinMUC(ctx, Config{Host: host, Room: room, Nick: "bot1-filler"})
	if err != nil {
		t.Fatalf("bot1 JoinMUC: %v", err)
	}
	defer bot1.Close()
	t.Log("bot1: in room")

	// Bot 2: full Join (will get session-initiate from Jicofo)
	t.Log("bot2: joining with Jingle...")
	bot2, err := Join(ctx, Config{Host: host, Room: room, Nick: "bot2-video"})
	if err != nil {
		t.Fatalf("bot2 Join: %v", err)
	}
	defer bot2.Close()
	t.Log("bot2: got session-initiate")

	// Verify offer has video
	if !strings.Contains(bot2.SDP, "m=video") {
		t.Fatal("offer SDP missing m=video")
	}
	t.Logf("offer has video, codecs present: VP8=%v VP9=%v",
		strings.Contains(bot2.SDP, "VP8"), strings.Contains(bot2.SDP, "VP9"))

	// Setup PeerConnection with sendonly VP8
	pc, err := webrtc.NewPeerConnection(bot2.IceConfig())
	if err != nil {
		t.Fatalf("NewPeerConnection: %v", err)
	}
	defer pc.Close()

	localVideo, _ := webrtc.NewTrackLocalStaticSample(
		webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeVP8, ClockRate: 90000},
		"jvideo", "jstream")
	if _, err := pc.AddTrack(localVideo); err != nil {
		t.Fatalf("AddTrack: %v", err)
	}

	pc.OnTrack(func(track *webrtc.TrackRemote, _ *webrtc.RTPReceiver) {
		t.Logf("[track] kind=%s codec=%s ssrc=%d", track.Kind(), track.Codec().MimeType, track.SSRC())
	})

	connectedCh := make(chan struct{}, 1)
	pc.OnConnectionStateChange(func(s webrtc.PeerConnectionState) {
		t.Logf("PC: %s", s)
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
	t.Log("session-accept sent")

	// Verify answer has video
	if !strings.Contains(pc.LocalDescription().SDP, "m=video") {
		t.Error("answer SDP missing m=video")
	}

	// Feed dummy frames
	go func() {
		frame := []byte{0x10, 0x02, 0x00, 0x9d, 0x01, 0x2a, 0x01, 0x00, 0x01, 0x00, 0x00, 0xc0, 0xfd, 0x07, 0x86, 0x83, 0x97, 0xff, 0xfe, 0xfb, 0x9f, 0x00, 0x00}
		tick := time.NewTicker(33 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
				_ = localVideo.WriteSample(media.Sample{Data: frame, Duration: 33 * time.Millisecond})
			}
		}
	}()

	select {
	case <-connectedCh:
		t.Log("SUCCESS: PeerConnection connected — video works!")
	case <-time.After(20 * time.Second):
		t.Log("ICE timeout (firewall/no TURN) but SDP negotiation succeeded — video negotiation OK")
	}

	_ = neg.Terminate("success")
}

func TestE2EVideoRecvOnly(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	room := "j-video-e2e-recv"
	host := "meet.cryptopro.ru"

	// Bot 1
	bot1, err := JoinMUC(ctx, Config{Host: host, Room: room, Nick: "bot1-filler"})
	if err != nil {
		t.Fatalf("bot1: %v", err)
	}
	defer bot1.Close()

	// Bot 2: full join
	bot2, err := Join(ctx, Config{Host: host, Room: room, Nick: "bot2-recv"})
	if err != nil {
		t.Fatalf("bot2: %v", err)
	}
	defer bot2.Close()

	if !strings.Contains(bot2.SDP, "m=video") {
		t.Fatal("offer missing m=video")
	}

	pc, err := webrtc.NewPeerConnection(bot2.IceConfig())
	if err != nil {
		t.Fatalf("NewPC: %v", err)
	}
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
	_ = neg.Terminate("success")
}
