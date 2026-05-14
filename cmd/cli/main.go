package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"time"

	"github.com/pion/webrtc/v4"
	j "github.com/zarazaex69/j"
	"github.com/zarazaex69/j/internal/colibri"
)

func main() {
	host := flag.String("host", "", "Jitsi Meet server host")
	room := flag.String("room", "", "Room name")
	nick := flag.String("nick", "thejproject", "Display name")
	debug := flag.Bool("debug", false, "Verbose XMPP logging")
	chat := flag.Bool("chat", false, "Chat mode: join room and read stdin for messages")
	dc := flag.Bool("dc", false, "Bridge channel mode: stdin → broadcast EndpointMessage as text")
	dcRaw := flag.Bool("dc-raw", false, "Bridge channel raw mode: pipe stdin → bridge → stdout (binary, base64-framed)")
	media := flag.Bool("media", false, "Media mode: setup pion PeerConnection, send session-accept, print track events")
	timeout := flag.Duration("timeout", 5*time.Minute, "Timeout waiting for Jingle session")
	flag.Parse()

	if *host == "" || *room == "" {
		fmt.Fprintln(os.Stderr, "usage: cli -host meet.example.com -room myroom [-nick name] [-chat | -dc | -dc-raw]")
		os.Exit(1)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	fmt.Fprintf(os.Stderr, "joining %s/%s as %s...\n", *host, *room, *nick)

	switch {
	case *media:
		runMedia(ctx, *host, *room, *nick, *debug, *timeout)
	case *dcRaw:
		runDCRaw(ctx, *host, *room, *nick, *debug, *timeout)
	case *dc:
		runDC(ctx, *host, *room, *nick, *debug, *timeout)
	case *chat:
		runChat(ctx, *host, *room, *nick, *debug)
	default:
		runJingle(ctx, *host, *room, *nick, *debug, *timeout)
	}
}

func runChat(ctx context.Context, host, room, nick string, debug bool) {
	sess, err := j.JoinMUC(ctx, j.Config{Host: host, Room: room, Nick: nick, Debug: debug})
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	defer sess.Close()

	fmt.Fprintf(os.Stderr, "joined! type messages (/raise, /lower, /quit):\n")

	lines := readLines(ctx)
	incoming := sess.Messages()

	for {
		select {
		case <-ctx.Done():
			fmt.Fprintln(os.Stderr, "\nbye")
			return
		case m, ok := <-incoming:
			if !ok {
				return
			}
			fmt.Printf("<%s> %s\n", m.From, m.Body)
		case line, ok := <-lines:
			if !ok {
				return
			}
			if line == "" {
				continue
			}
			switch line {
			case "/quit", "/exit", "/leave":
				return
			case "/raise":
				if err := sess.RaiseHand(); err != nil {
					fmt.Fprintf(os.Stderr, "error: %v\n", err)
				}
			case "/lower":
				if err := sess.LowerHand(); err != nil {
					fmt.Fprintf(os.Stderr, "error: %v\n", err)
				}
			default:
				if err := sess.Chat(line); err != nil {
					fmt.Fprintf(os.Stderr, "send error: %v\n", err)
					return
				}
			}
		}
	}
}

func runJingle(ctx context.Context, host, room, nick string, debug bool, timeout time.Duration) {
	ctx, tcancel := context.WithTimeout(ctx, timeout)
	defer tcancel()

	sess, err := j.Join(ctx, j.Config{Host: host, Room: room, Nick: nick, Debug: debug})
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	defer sess.Close()

	out := map[string]any{
		"jid":          sess.JID,
		"room_jid":     sess.RoomJID,
		"colibri_ws":   sess.ColibriWS,
		"sdp":          sess.SDP,
		"ice_servers":  sess.ICEServers,
		"candidates":   sess.Candidates,
		"data_channel": sess.DataChannel,
		"audio_ssrc":   sess.AudioSSRC,
		"video_ssrc":   sess.VideoSSRC,
	}

	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	enc.Encode(out)
}

// runDC: text broadcast over bridge channel. Each line of stdin → EndpointMessage{text:line}.
func runDC(ctx context.Context, host, room, nick string, debug bool, timeout time.Duration) {
	jctx, jcancel := context.WithTimeout(ctx, timeout)
	defer jcancel()

	fmt.Fprintln(os.Stderr, "waiting for jingle session-initiate (needs 2nd participant in room)...")
	sess, err := j.Join(jctx, j.Config{Host: host, Room: room, Nick: nick, Debug: debug})
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	defer sess.Close()

	if err := sess.OpenBridge(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "open bridge: %v\n", err)
		os.Exit(1)
	}

	fmt.Fprintln(os.Stderr, "bridge connected. type messages to broadcast, /quit to exit:")

	go func() {
		for m := range sess.BridgeMessages() {
			fmt.Printf("[%s/%s] %s\n", m.Class, m.From, string(m.RawJSON))
		}
	}()

	lines := readLines(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case line, ok := <-lines:
			if !ok {
				return
			}
			if line == "/quit" || line == "/exit" {
				return
			}
			if err := sess.BridgeSendMessage("", map[string]any{"text": line}); err != nil {
				fmt.Fprintf(os.Stderr, "send: %v\n", err)
				return
			}
		}
	}
}

// runDCRaw: pipe arbitrary binary through the bridge.
//   stdin (binary) → SendRaw broadcast → other endpoint receives → DecodeRaw → stdout
func runDCRaw(ctx context.Context, host, room, nick string, debug bool, timeout time.Duration) {
	jctx, jcancel := context.WithTimeout(ctx, timeout)
	defer jcancel()

	fmt.Fprintln(os.Stderr, "waiting for jingle session-initiate (needs 2nd participant in room)...")
	sess, err := j.Join(jctx, j.Config{Host: host, Room: room, Nick: nick, Debug: debug})
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	defer sess.Close()

	if err := sess.OpenBridge(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "open bridge: %v\n", err)
		os.Exit(1)
	}

	fmt.Fprintln(os.Stderr, "bridge connected. raw mode: stdin (bytes) ←→ bridge ←→ stdout")

	// receive: decode raw frames to stdout, log other classes to stderr
	go func() {
		for m := range sess.BridgeMessages() {
			if raw := colibri.DecodeRaw(m); raw != nil {
				os.Stdout.Write(raw)
				continue
			}
			if m.Class != "EndpointMessage" {
				fmt.Fprintf(os.Stderr, "[%s] %s\n", m.Class, string(m.RawJSON))
			}
		}
	}()

	// send: chunked stdin
	buf := make([]byte, 8192)
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		n, err := os.Stdin.Read(buf)
		if n > 0 {
			if serr := sess.BridgeSendRaw("", buf[:n]); serr != nil {
				fmt.Fprintf(os.Stderr, "send: %v\n", serr)
				return
			}
		}
		if err != nil {
			return
		}
	}
}

func runMedia(ctx context.Context, host, room, nick string, debug bool, timeout time.Duration) {
	jctx, jcancel := context.WithTimeout(ctx, timeout)
	defer jcancel()

	fmt.Fprintln(os.Stderr, "joining and waiting for jingle session-initiate...")
	sess, err := j.Join(jctx, j.Config{Host: host, Room: room, Nick: nick, Debug: debug})
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	defer sess.Close()

	pc, err := webrtc.NewPeerConnection(sess.IceConfig())
	if err != nil {
		fmt.Fprintf(os.Stderr, "new pc: %v\n", err)
		os.Exit(1)
	}
	defer pc.Close()

	// add a recvonly transceiver per media (so SDP answer matches offer order)
	if _, err := pc.AddTransceiverFromKind(webrtc.RTPCodecTypeAudio, webrtc.RTPTransceiverInit{Direction: webrtc.RTPTransceiverDirectionRecvonly}); err != nil {
		fmt.Fprintf(os.Stderr, "add audio recvonly: %v\n", err)
	}
	if _, err := pc.AddTransceiverFromKind(webrtc.RTPCodecTypeVideo, webrtc.RTPTransceiverInit{Direction: webrtc.RTPTransceiverDirectionRecvonly}); err != nil {
		fmt.Fprintf(os.Stderr, "add video recvonly: %v\n", err)
	}

	pc.OnTrack(func(track *webrtc.TrackRemote, _ *webrtc.RTPReceiver) {
		fmt.Fprintf(os.Stderr, "[track] kind=%s id=%s codec=%s ssrc=%d\n",
			track.Kind(), track.ID(), track.Codec().MimeType, track.SSRC())
		// drain so SRTP isn't stuck
		buf := make([]byte, 1500)
		var pkts uint64
		for {
			_, _, err := track.Read(buf)
			if err != nil {
				fmt.Fprintf(os.Stderr, "[track ssrc=%d] closed: %v (rx=%d packets)\n", track.SSRC(), err, pkts)
				return
			}
			pkts++
		}
	})

	pc.OnICEConnectionStateChange(func(s webrtc.ICEConnectionState) {
		fmt.Fprintf(os.Stderr, "ICE: %s\n", s)
	})
	pc.OnConnectionStateChange(func(s webrtc.PeerConnectionState) {
		fmt.Fprintf(os.Stderr, "PC: %s\n", s)
	})

	neg := sess.Negotiator()
	neg.PC = pc
	if err := neg.Accept(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "accept: %v\n", err)
		os.Exit(1)
	}

	fmt.Fprintln(os.Stderr, "session-accept sent. waiting (Ctrl+C to quit)…")
	<-ctx.Done()
}

func readLines(ctx context.Context) <-chan string {
	out := make(chan string)
	go func() {
		defer close(out)
		scanner := bufio.NewScanner(os.Stdin)
		for scanner.Scan() {
			select {
			case out <- scanner.Text():
			case <-ctx.Done():
				return
			}
		}
	}()
	return out
}
