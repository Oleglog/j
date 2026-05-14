package j

import (
	"context"
	"fmt"

	"github.com/zarazaex69/j/internal/jingle"
	"github.com/zarazaex69/j/internal/xmpp"
)

type Config struct {
	Host  string // e.g. "meet.cryptopro.ru"
	Room  string // e.g. "myroom"
	Nick  string // display name
	Debug bool   // verbose XMPP logging
}

type ICEServer struct {
	URLs       []string
	Username   string
	Credential string
}

type Session struct {
	JID          string
	RoomJID      string
	SDP          string // remote SDP offer
	ICEServers   []ICEServer
	Candidates   []jingle.Candidate
	DataChannel  *jingle.DataChannel
	AudioSSRC    []jingle.Source
	VideoSSRC    []jingle.Source
	Conn         *xmpp.Conn
	room         string
	jingleSID    string
	initiator    string
}

func (s *Session) Accept(sdp string) error {
	return s.Conn.SendSessionAccept(s.jingleSID, s.initiator, s.RoomJID, sdp)
}

func (s *Session) Chat(msg string) error {
	return s.Conn.SendGroupchat(s.RoomJID, msg)
}

func (s *Session) RaiseHand() error {
	return s.Conn.RaiseHand(s.room)
}

func (s *Session) LowerHand() error {
	return s.Conn.LowerHand(s.room)
}

func (s *Session) Close() error {
	return s.Conn.Close()
}

// JoinMUC connects to the room without waiting for Jingle session.
func JoinMUC(ctx context.Context, cfg Config) (*Session, error) {
	if cfg.Host == "" || cfg.Room == "" {
		return nil, fmt.Errorf("host and room are required")
	}
	if cfg.Nick == "" {
		cfg.Nick = "j-client"
	}

	conn, err := xmpp.Dial(ctx, cfg.Host, cfg.Room, cfg.Debug)
	if err != nil {
		return nil, fmt.Errorf("xmpp dial: %w", err)
	}

	if err := conn.AllocateFocus(cfg.Room); err != nil {
		conn.Close()
		return nil, fmt.Errorf("allocate focus: %w", err)
	}

	if err := conn.JoinMUC(cfg.Room, cfg.Nick); err != nil {
		conn.Close()
		return nil, fmt.Errorf("join muc: %w", err)
	}

	return &Session{
		JID:     conn.JID(),
		RoomJID: fmt.Sprintf("%s@conference.%s", cfg.Room, cfg.Host),
		Conn:    conn,
		room:    cfg.Room,
	}, nil
}

func Join(ctx context.Context, cfg Config) (*Session, error) {
	if cfg.Host == "" || cfg.Room == "" {
		return nil, fmt.Errorf("host and room are required")
	}
	if cfg.Nick == "" {
		cfg.Nick = "j-client"
	}

	conn, err := xmpp.Dial(ctx, cfg.Host, cfg.Room, cfg.Debug)
	if err != nil {
		return nil, fmt.Errorf("xmpp dial: %w", err)
	}

	services, err := conn.DiscoverServices()
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("discover services: %w", err)
	}

	if err := conn.AllocateFocus(cfg.Room); err != nil {
		conn.Close()
		return nil, fmt.Errorf("allocate focus: %w", err)
	}

	if err := conn.JoinMUC(cfg.Room, cfg.Nick); err != nil {
		conn.Close()
		return nil, fmt.Errorf("join muc: %w", err)
	}

	ji, err := conn.WaitJingle(ctx)
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("wait jingle: %w", err)
	}

	parsed := jingle.Parse(ji)

	sess := &Session{
		JID:         conn.JID(),
		RoomJID:     fmt.Sprintf("%s@conference.%s", cfg.Room, cfg.Host),
		SDP:         parsed.SDP,
		ICEServers:  convertICE(services),
		Candidates:  parsed.Candidates,
		DataChannel: parsed.DataChannel,
		AudioSSRC:   parsed.AudioSources,
		VideoSSRC:   parsed.VideoSources,
		Conn:        conn,
		room:        cfg.Room,
		jingleSID:   parsed.SID,
		initiator:   parsed.Initiator,
	}

	return sess, nil
}

func convertICE(services []xmpp.Service) []ICEServer {
	var servers []ICEServer
	for _, s := range services {
		var url string
		switch s.Type {
		case "stun":
			url = fmt.Sprintf("stun:%s:%s", s.Host, s.Port)
		case "turn":
			url = fmt.Sprintf("turn:%s:%s?transport=%s", s.Host, s.Port, s.Transport)
		case "turns":
			url = fmt.Sprintf("turns:%s:%s?transport=%s", s.Host, s.Port, s.Transport)
		default:
			continue
		}
		servers = append(servers, ICEServer{
			URLs:       []string{url},
			Username:   s.Username,
			Credential: s.Password,
		})
	}
	return servers
}
