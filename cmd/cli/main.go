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

	j "github.com/zarazaex69/j"
	"github.com/pion/webrtc/v4"
)

func main() {
	host := flag.String("host", "", "Jitsi Meet server host")
	room := flag.String("room", "", "Room name")
	nick := flag.String("nick", "thejproject", "Display name")
	debug := flag.Bool("debug", false, "Verbose XMPP logging")
	chat := flag.Bool("chat", false, "Chat mode: join room and read stdin for messages")
	dc := flag.Bool("dc", false, "DataChannel PoC: wait for Jingle, set up PeerConnection, send messages from stdin")
	timeout := flag.Duration("timeout", 5*time.Minute, "Timeout waiting for Jingle session")
	flag.Parse()

	if *host == "" || *room == "" {
		fmt.Fprintln(os.Stderr, "usage: cli -host meet.example.com -room myroom [-nick name] [-chat | -dc]")
		os.Exit(1)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	fmt.Fprintf(os.Stderr, "joining %s/%s as %s...\n", *host, *room, *nick)

	switch {
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

	lines := make(chan string)
	go func() {
		scanner := bufio.NewScanner(os.Stdin)
		for scanner.Scan() {
			lines <- scanner.Text()
		}
		close(lines)
	}()

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

	if sess.DataChannel == nil {
		fmt.Fprintln(os.Stderr, "no data channel in jingle offer")
		os.Exit(1)
	}

	fmt.Fprintln(os.Stderr, "got jingle offer, building peer connection...")

	// build pion ICE config
	var iceServers []webrtc.ICEServer
	for _, s := range sess.ICEServers {
		iceServers = append(iceServers, webrtc.ICEServer{
			URLs:       s.URLs,
			Username:   s.Username,
			Credential: s.Credential,
		})
	}

	pc, err := webrtc.NewPeerConnection(webrtc.Configuration{ICEServers: iceServers})
	if err != nil {
		fmt.Fprintf(os.Stderr, "new peer connection: %v\n", err)
		os.Exit(1)
	}
	defer pc.Close()

	pc.OnICEConnectionStateChange(func(state webrtc.ICEConnectionState) {
		fmt.Fprintf(os.Stderr, "ICE state: %s\n", state)
	})
	pc.OnConnectionStateChange(func(state webrtc.PeerConnectionState) {
		fmt.Fprintf(os.Stderr, "PC state: %s\n", state)
	})

	// open datachannel
	dc, err := pc.CreateDataChannel("j-poc", nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "create dc: %v\n", err)
		os.Exit(1)
	}
	dc.OnOpen(func() {
		fmt.Fprintln(os.Stderr, "DataChannel open!")
	})
	dc.OnMessage(func(msg webrtc.DataChannelMessage) {
		if msg.IsString {
			fmt.Printf("[dc] %s\n", string(msg.Data))
		} else {
			fmt.Printf("[dc binary] %d bytes\n", len(msg.Data))
		}
	})
	dc.OnClose(func() {
		fmt.Fprintln(os.Stderr, "DataChannel closed")
	})

	// set the remote jingle SDP as offer
	if err := pc.SetRemoteDescription(webrtc.SessionDescription{
		Type: webrtc.SDPTypeOffer,
		SDP:  sess.SDP,
	}); err != nil {
		fmt.Fprintf(os.Stderr, "set remote desc: %v\n", err)
		fmt.Fprintln(os.Stderr, "--- offer SDP ---")
		fmt.Fprintln(os.Stderr, sess.SDP)
		os.Exit(1)
	}

	// create answer
	answer, err := pc.CreateAnswer(nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "create answer: %v\n", err)
		os.Exit(1)
	}
	if err := pc.SetLocalDescription(answer); err != nil {
		fmt.Fprintf(os.Stderr, "set local desc: %v\n", err)
		os.Exit(1)
	}

	// wait for ICE gathering
	<-webrtc.GatheringCompletePromise(pc)

	finalAnswer := pc.LocalDescription().SDP

	// send session-accept back through XMPP (raw SDP wrapped)
	jingleAccept := sdpToJingleAccept(finalAnswer)
	if err := sess.Accept(jingleAccept); err != nil {
		fmt.Fprintf(os.Stderr, "send accept: %v\n", err)
		os.Exit(1)
	}

	fmt.Fprintln(os.Stderr, "session-accept sent. type messages, /quit to exit:")

	lines := make(chan string)
	go func() {
		scanner := bufio.NewScanner(os.Stdin)
		for scanner.Scan() {
			lines <- scanner.Text()
		}
		close(lines)
	}()

	for {
		select {
		case <-ctx.Done():
			fmt.Fprintln(os.Stderr, "\nbye")
			return
		case line, ok := <-lines:
			if !ok {
				return
			}
			if line == "/quit" || line == "/exit" {
				return
			}
			if dc.ReadyState() != webrtc.DataChannelStateOpen {
				fmt.Fprintf(os.Stderr, "dc not open yet (state=%s)\n", dc.ReadyState())
				continue
			}
			if err := dc.SendText(line); err != nil {
				fmt.Fprintf(os.Stderr, "dc send: %v\n", err)
			}
		}
	}
}

// sdpToJingleAccept is a placeholder — real Jitsi expects Jingle XML with
// content/description elements, not raw SDP. This PoC just dumps the SDP
// for now; full conversion needs an SDP→Jingle translator.
func sdpToJingleAccept(sdp string) string {
	return ""
}
