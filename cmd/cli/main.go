package main

import (
	"bufio"
	"context"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"time"

	"github.com/coder/websocket"
	j "github.com/zarazaex69/j"
)

func main() {
	host := flag.String("host", "", "Jitsi Meet server host")
	room := flag.String("room", "", "Room name")
	nick := flag.String("nick", "thejproject", "Display name")
	debug := flag.Bool("debug", false, "Verbose XMPP logging")
	chat := flag.Bool("chat", false, "Chat mode: join room and read stdin for messages")
	dc := flag.Bool("dc", false, "DataChannel PoC: wait for Jingle, set up PeerConnection, send messages from stdin")
	dcRaw := flag.Bool("dc-raw", false, "DataChannel PoC raw mode: pipe stdin as binary base64 frames, print received frames decoded to stdout")
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

	if sess.ColibriWS == "" {
		fmt.Fprintln(os.Stderr, "no colibri-ws URL in jingle offer")
		os.Exit(1)
	}

	wsConn, _, err := websocket.Dial(ctx, sess.ColibriWS, nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "colibri ws dial: %v\n", err)
		os.Exit(1)
	}
	defer wsConn.Close(websocket.StatusNormalClosure, "")

	fmt.Fprintln(os.Stderr, "colibri-ws connected. raw mode: stdin chunks → base64 → broadcast.")

	// reader: parse incoming EndpointMessage and decode b64 -> stdout
	go func() {
		for {
			_, data, err := wsConn.Read(ctx)
			if err != nil {
				return
			}
			var msg map[string]any
			if err := json.Unmarshal(data, &msg); err != nil {
				continue
			}
			if msg["colibriClass"] != "EndpointMessage" {
				continue
			}
			b64, ok := msg["b64"].(string)
			if !ok {
				continue
			}
			raw, err := base64.StdEncoding.DecodeString(b64)
			if err != nil {
				continue
			}
			os.Stdout.Write(raw)
		}
	}()

	// stdin: read 4KB chunks, send each as one EndpointMessage
	buf := make([]byte, 4096)
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		n, err := os.Stdin.Read(buf)
		if n > 0 {
			payload := map[string]any{
				"colibriClass": "EndpointMessage",
				"to":           "",
				"b64":          base64.StdEncoding.EncodeToString(buf[:n]),
			}
			data, _ := json.Marshal(payload)
			if werr := wsConn.Write(ctx, websocket.MessageText, data); werr != nil {
				fmt.Fprintf(os.Stderr, "ws send: %v\n", werr)
				return
			}
		}
		if err != nil {
			return
		}
	}
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

	if sess.ColibriWS == "" {
		fmt.Fprintln(os.Stderr, "no colibri-ws URL in jingle offer (this Jitsi may not support it)")
		os.Exit(1)
	}

	fmt.Fprintf(os.Stderr, "connecting to colibri-ws: %s\n", sess.ColibriWS)

	wsConn, _, err := websocket.Dial(ctx, sess.ColibriWS, nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "colibri ws dial: %v\n", err)
		os.Exit(1)
	}
	defer wsConn.Close(websocket.StatusNormalClosure, "")

	fmt.Fprintln(os.Stderr, "colibri-ws connected. type messages to broadcast, /quit to exit:")

	// reader
	go func() {
		for {
			_, data, err := wsConn.Read(ctx)
			if err != nil {
				fmt.Fprintf(os.Stderr, "ws read error: %v\n", err)
				return
			}
			fmt.Printf("[colibri] %s\n", string(data))
		}
	}()

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
			return
		case line, ok := <-lines:
			if !ok {
				return
			}
			if line == "/quit" || line == "/exit" {
				return
			}
			payload := map[string]any{
				"colibriClass": "EndpointMessage",
				"to":           "", // broadcast
				"msgPayload": map[string]any{
					"text": line,
				},
			}
			data, _ := json.Marshal(payload)
			if err := wsConn.Write(ctx, websocket.MessageText, data); err != nil {
				fmt.Fprintf(os.Stderr, "ws send: %v\n", err)
				return
			}
		}
	}
}
