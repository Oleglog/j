package peer

import (
	"strings"
	"testing"
)

func TestStripRemoteSourcesForPion(t *testing.T) {
	sdp := strings.Join([]string{
		"v=0",
		"o=- 0 2 IN IP4 0.0.0.0",
		"m=video 9 UDP/TLS/RTP/SAVPF 96",
		"a=mid:video",
		"a=rtpmap:96 VP8/90000",
		"a=ssrc-group:FID 1111 2222",
		"a=ssrc:1111 cname:remote",
		"a=ssrc:2222 cname:remote",
		"",
	}, "\r\n")

	got := stripRemoteSourcesForPion(sdp)
	if strings.Contains(got, "a=ssrc:") {
		t.Fatalf("stripRemoteSourcesForPion kept SSRC line:\n%s", got)
	}
	if strings.Contains(got, "a=ssrc-group:") {
		t.Fatalf("stripRemoteSourcesForPion kept SSRC group:\n%s", got)
	}
	if !strings.Contains(got, "a=rtpmap:96 VP8/90000") {
		t.Fatalf("stripRemoteSourcesForPion removed non-source media lines:\n%s", got)
	}
	if sdpHasExplicitSources(got) {
		t.Fatalf("sdpHasExplicitSources(%q) = true, want false", got)
	}
}

func TestSDPHasExplicitSources(t *testing.T) {
	if sdpHasExplicitSources("v=0\r\nm=video 9 UDP/TLS/RTP/SAVPF 96\r\n") {
		t.Fatal("source-less SDP reported explicit sources")
	}
	if !sdpHasExplicitSources("v=0\r\na=ssrc:1234 cname:remote\r\n") {
		t.Fatal("SDP with a=ssrc was not reported as explicit")
	}
	if !sdpHasExplicitSources("v=0\r\na=ssrc-group:FID 1234 5678\r\n") {
		t.Fatal("SDP with a=ssrc-group was not reported as explicit")
	}
}
