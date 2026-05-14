package main

import (
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
	host := flag.String("host", "", "Jitsi Meet server host (e.g. meet.example.com)")
	room := flag.String("room", "", "Room name")
	nick := flag.String("nick", "thejproject", "Display name")
	debug := flag.Bool("debug", false, "Verbose XMPP logging")
	timeout := flag.Duration("timeout", 60*time.Second, "Timeout waiting for Jingle session")
	flag.Parse()

	if *host == "" || *room == "" {
		fmt.Fprintln(os.Stderr, "usage: cli -host meet.example.com -room myroom [-nick name]")
		os.Exit(1)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	ctx, tcancel := context.WithTimeout(ctx, *timeout)
	defer tcancel()

	fmt.Fprintf(os.Stderr, "joining %s/%s as %s...\n", *host, *room, *nick)

	sess, err := j.Join(ctx, j.Config{Host: *host, Room: *room, Nick: *nick, Debug: *debug})
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	defer sess.Close()

	out := map[string]any{
		"jid":         sess.JID,
		"room_jid":    sess.RoomJID,
		"sdp":         sess.SDP,
		"ice_servers":  sess.ICEServers,
		"candidates":  sess.Candidates,
		"data_channel": sess.DataChannel,
		"audio_ssrc":  sess.AudioSSRC,
		"video_ssrc":  sess.VideoSSRC,
	}

	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	enc.Encode(out)
}
