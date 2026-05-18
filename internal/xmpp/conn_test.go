package xmpp

import (
	"strings"
	"testing"
)

func TestJitsiCapsVersion(t *testing.T) {
	got := jitsiCapsVersion
	const want = "xUTPQ7C5wic8q6nOytcEI8dwRJI="
	if got != want {
		t.Fatalf("jitsiCapsVersion = %q, want %q", got, want)
	}
}

func TestIsDiscoInfoGetAcceptsQuoteStyles(t *testing.T) {
	single := `<iq from='focus.meet.example' id='abc' type='get'><query xmlns='http://jabber.org/protocol/disco#info'/></iq>`
	double := `<iq from="focus.meet.example" id="abc" type="get"><query xmlns="http://jabber.org/protocol/disco#info"/></iq>`

	if !isDiscoInfoGet(single) {
		t.Fatal("single-quoted disco#info get was not detected")
	}
	if !isDiscoInfoGet(double) {
		t.Fatal("double-quoted disco#info get was not detected")
	}
}

func TestIsSessionInitiate(t *testing.T) {
	stanza := `<iq from='room@conference.example/focus' type='set'><jingle xmlns='urn:xmpp:jingle:1' action='session-initiate'/></iq>`
	if !isSessionInitiate(stanza) {
		t.Fatal("session-initiate stanza was not detected")
	}
}

func TestDiscoFeatureXMLIncludesModernJitsiFeatures(t *testing.T) {
	xml := discoFeatureXML()
	for _, feature := range []string{
		"http://jitsi.org/json-encoded-sources",
		"http://jitsi.org/source-name",
		"http://jitsi.org/receive-multiple-video-streams",
		"urn:xmpp:jingle:apps:rtp:video",
	} {
		if !strings.Contains(xml, `var="`+feature+`"`) {
			t.Fatalf("disco feature %q missing from %s", feature, xml)
		}
	}
}
