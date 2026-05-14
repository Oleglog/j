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
)

func main() {
	host := flag.String("host", "", "Jitsi Meet server host")
	room := flag.String("room", "", "Room name")
	nick := flag.String("nick", "thejproject", "Display name")
	debug := flag.Bool("debug", false, "Verbose XMPP logging")
	chat := flag.Bool("chat", false, "Chat mode: join room and read stdin for messages")
	timeout := flag.Duration("timeout", 60*time.Second, "Timeout waiting for Jingle session")
	flag.Parse()

	if *host == "" || *room == "" {
		fmt.Fprintln(os.Stderr, "usage: cli -host meet.example.com -room myroom [-nick name] [-chat]")
		os.Exit(1)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	fmt.Fprintf(os.Stderr, "joining %s/%s as %s...\n", *host, *room, *nick)

	if *chat {
		runChat(ctx, *host, *room, *nick, *debug)
	} else {
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

	fmt.Fprintf(os.Stderr, "joined! type messages (/raise, /lower for hand):\n")

	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}
		switch line {
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
