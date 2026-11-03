package quark

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	cloudstorage "github.com/Niexiawei/simple-cloud-storage"
)

func TestUploadOSSEndpoint(t *testing.T) {
	for _, tc := range []struct {
		name, raw, want string
	}{
		{"quark pds", "http://pds.quark.cn", "https://bucket.pds.quark.cn/object/key"},
		{"quark pds https", "https://pds.quark.cn", "https://bucket.pds.quark.cn/object/key"},
		{"quark bucket endpoint", "https://bucket.pds.quark.cn", "https://bucket.pds.quark.cn/object/key"},
		{"alibaba oss", "http://oss-cn-hangzhou.aliyuncs.com", "https://bucket.oss-cn-hangzhou.aliyuncs.com/object/key"},
		{"untrusted", "https://example.com", ""},
		{"domain suffix spoof", "https://pds.quark.cn.example.com", ""},
		{"signed query", "https://pds.quark.cn?token=secret", ""},
		{"userinfo", "https://secret@pds.quark.cn", ""},
		{"invalid scheme", "ftp://pds.quark.cn", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			u, err := ossURL(preData{UploadURL: tc.raw, Bucket: "bucket", ObjectKey: "object/key"}, false)
			if tc.want == "" {
				if !errors.Is(err, cloudstorage.ErrProtocol) {
					t.Fatalf("untrusted endpoint accepted: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if u.String() != tc.want {
				t.Fatalf("OSS endpoint = %s, want %s", u, tc.want)
			}
		})
	}
}

func TestDownloadLinkUsesSameClientUserAgentAsDataRequest(t *testing.T) {
	f := newFake(t)
	cfg := f.cfg(t)
	base := fakeTransport{f}
	linkCalls, dataCalls := 0, 0
	cfg.HTTPClient.Transport = roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.URL.Path == "/api/file/download" {
			linkCalls++
			if r.Header.Get("User-Agent") != userAgent {
				return &http.Response{StatusCode: 400, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{"status":400,"code":23018}`))}, nil
			}
		}
		if strings.HasPrefix(r.URL.Path, "/blob/") {
			dataCalls++
			if r.Header.Get("User-Agent") != userAgent {
				t.Error("signed link and data user agents differ")
			}
		}
		return base.RoundTrip(r)
	})
	c := connectFake(t, f, cfg)
	if _, err := c.Upload(context.Background(), "a.mp4", strings.NewReader("abcd"), cloudstorage.UploadOptions{}); err != nil {
		t.Fatal(err)
	}
	if linkCalls == 0 || dataCalls == 0 {
		t.Fatal("did not validate a signed download")
	}
}
