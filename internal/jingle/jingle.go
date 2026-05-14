package jingle

import (
	"encoding/xml"
	"strings"
)

type Candidate struct {
	Component  string
	Foundation string
	Generation string
	ID         string
	IP         string
	Port       string
	Priority   string
	Protocol   string
	Type       string
	RelAddr    string
	RelPort    string
}

type Source struct {
	SSRC  string
	Name  string
	Label string
}

type DataChannel struct {
	Port     string
	Protocol string
}

type Parsed struct {
	SID          string
	Initiator    string
	SDP          string
	Jingle       *XMLJingle
	Candidates   []Candidate
	AudioSources []Source
	VideoSources []Source
	DataChannel  *DataChannel
}

func Parse(raw string) *Parsed {
	p := &Parsed{}

	// extract sid and initiator from jingle element
	p.SID = extractAttr(raw, "sid")
	p.Initiator = extractAttr(raw, "initiator")

	// parse XML structure
	type xmlCandidate struct {
		Component  string `xml:"component,attr"`
		Foundation string `xml:"foundation,attr"`
		Generation string `xml:"generation,attr"`
		ID         string `xml:"id,attr"`
		IP         string `xml:"ip,attr"`
		Port       string `xml:"port,attr"`
		Priority   string `xml:"priority,attr"`
		Protocol   string `xml:"protocol,attr"`
		Type       string `xml:"type,attr"`
		RelAddr    string `xml:"rel-addr,attr"`
		RelPort    string `xml:"rel-port,attr"`
	}
	type xmlFingerprint struct {
		Hash    string `xml:"hash,attr"`
		Setup   string `xml:"setup,attr"`
		Content string `xml:",chardata"`
	}
	type xmlTransport struct {
		Ufrag       string           `xml:"ufrag,attr"`
		Pwd         string           `xml:"pwd,attr"`
		Candidates  []xmlCandidate   `xml:"candidate"`
		Fingerprint []xmlFingerprint `xml:"fingerprint"`
	}
	type xmlSource struct {
		SSRC   string `xml:"ssrc,attr"`
		Params []struct {
			Name  string `xml:"name,attr"`
			Value string `xml:"value,attr"`
		} `xml:"parameter"`
	}
	type xmlDescription struct {
		Media   string      `xml:"media,attr"`
		Sources []xmlSource `xml:"source"`
	}
	type xmlSCTP struct {
		Port     string `xml:"port,attr"`
		Protocol string `xml:"protocol,attr"`
	}
	type xmlContent struct {
		Name        string           `xml:"name,attr"`
		Description xmlDescription   `xml:"description"`
		Transport   xmlTransport     `xml:"transport"`
		SCTP        []xmlSCTP        `xml:"transport>sctpmap"`
	}
	type xmlJingle struct {
		Contents []xmlContent `xml:"content"`
	}
	type xmlIQ struct {
		Jingle xmlJingle `xml:"jingle"`
	}

	var iq xmlIQ
	xml.Unmarshal([]byte(raw), &iq)

	var sdpLines []string
	sdpLines = append(sdpLines, "v=0", "o=- 0 0 IN IP4 0.0.0.0", "s=-", "t=0 0")

	for _, content := range iq.Jingle.Contents {
		for _, c := range content.Transport.Candidates {
			p.Candidates = append(p.Candidates, Candidate{
				Component:  c.Component,
				Foundation: c.Foundation,
				Generation: c.Generation,
				ID:         c.ID,
				IP:         c.IP,
				Port:       c.Port,
				Priority:   c.Priority,
				Protocol:   c.Protocol,
				Type:       c.Type,
				RelAddr:    c.RelAddr,
				RelPort:    c.RelPort,
			})
		}

		media := content.Description.Media
		if media == "" {
			media = content.Name
		}

		// build SDP media line
		port := "9"
		proto := "UDP/TLS/RTP/SAVPF"
		if content.Name == "data" {
			proto = "UDP/DTLS/SCTP"
			if len(content.SCTP) > 0 {
				p.DataChannel = &DataChannel{
					Port:     content.SCTP[0].Port,
					Protocol: content.SCTP[0].Protocol,
				}
				port = content.SCTP[0].Port
			}
		}
		sdpLines = append(sdpLines, "m="+media+" "+port+" "+proto+" 0")

		// ICE credentials
		if content.Transport.Ufrag != "" {
			sdpLines = append(sdpLines, "a=ice-ufrag:"+content.Transport.Ufrag)
			sdpLines = append(sdpLines, "a=ice-pwd:"+content.Transport.Pwd)
		}

		// fingerprint
		for _, fp := range content.Transport.Fingerprint {
			sdpLines = append(sdpLines, "a=fingerprint:"+fp.Hash+" "+strings.TrimSpace(fp.Content))
			sdpLines = append(sdpLines, "a=setup:"+fp.Setup)
		}

		// candidates
		for _, c := range content.Transport.Candidates {
			line := "a=candidate:" + c.Foundation + " " + c.Component + " " + c.Protocol + " " + c.Priority + " " + c.IP + " " + c.Port + " typ " + c.Type
			if c.RelAddr != "" {
				line += " raddr " + c.RelAddr + " rport " + c.RelPort
			}
			sdpLines = append(sdpLines, line)
		}

		// sources
		for _, src := range content.Description.Sources {
			var name, label string
			for _, param := range src.Params {
				switch param.Name {
				case "msid":
					name = param.Value
				case "label":
					label = param.Value
				}
			}
			s := Source{SSRC: src.SSRC, Name: name, Label: label}
			switch media {
			case "audio":
				p.AudioSources = append(p.AudioSources, s)
			case "video":
				p.VideoSources = append(p.VideoSources, s)
			}
		}
	}

	p.SDP = strings.Join(sdpLines, "\r\n") + "\r\n"
	return p
}

func extractAttr(s, attr string) string {
	key := attr + `="`
	i := strings.Index(s, key)
	if i == -1 {
		key = attr + `='`
		i = strings.Index(s, key)
		if i == -1 {
			return ""
		}
	}
	i += len(key)
	end := strings.IndexByte(s[i:], s[i-1])
	if end == -1 {
		return ""
	}
	return s[i : i+end]
}
