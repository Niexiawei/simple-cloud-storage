package quark

import (
	"context"
	"errors"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"

	cloudstorage "github.com/Niexiawei/simple-cloud-storage"
)

func TestConnectionAccountProtocol(t *testing.T) {
	for _, tc := range []struct {
		name, account, config, wantID, step string
		status                              int
		wantErr                             error
	}{
		{name: "string uid", account: `{"success":true,"data":{"uid":"account"}}`, wantID: "account"},
		{name: "numeric uid", account: `{"success":true,"data":{"uid":1234567890123456789}}`, wantID: "1234567890123456789"},
		{name: "account status namespace", account: `{"success":true,"status":2000000,"data":{"uid":"account"}}`, wantID: "account"},
		{name: "logged out", account: `{"success":false,"message":"secret-cookie https://signed","data":null}`, wantErr: cloudstorage.ErrAuthentication, step: "account info"},
		{name: "false success overrides numeric fields", account: `{"success":false,"status":200,"code":0,"data":{"uid":"account"}}`, wantErr: cloudstorage.ErrAuthentication, step: "account info"},
		{name: "missing success", account: `{"data":{"uid":"account"}}`, wantErr: cloudstorage.ErrProtocol, step: "account info"},
		{name: "empty uid", account: `{"success":true,"data":{"uid":""}}`, wantErr: cloudstorage.ErrAuthentication, step: "account info"},
		{name: "null uid", account: `{"success":true,"data":{"uid":null}}`, wantErr: cloudstorage.ErrAuthentication, step: "account info"},
		{name: "invalid json", account: `secret-cookie https://signed`, wantErr: cloudstorage.ErrProtocol, step: "account info"},
		{name: "http unauthorized", status: 401, account: `{"success":true,"data":{"uid":"account"}}`, wantErr: cloudstorage.ErrAuthentication, step: "account info"},
		{name: "drive must require status and code", account: `{"success":true,"data":{"uid":"account"}}`, config: `{"success":true,"data":{}}`, wantErr: cloudstorage.ErrProtocol, step: "refresh cookies"},
		{name: "drive must require status", account: `{"success":true,"data":{"uid":"account"}}`, config: `{"success":true,"code":0,"data":{}}`, wantErr: cloudstorage.ErrProtocol, step: "refresh cookies"},
		{name: "drive must require code", account: `{"success":true,"data":{"uid":"account"}}`, config: `{"success":true,"status":200,"data":{}}`, wantErr: cloudstorage.ErrProtocol, step: "refresh cookies"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFake(t)
			cfg := f.cfg(t)
			cfg.AccountURL = f.server.URL + "/account"
			base := fakeTransport{f}
			cfg.HTTPClient.Transport = roundTripFunc(func(r *http.Request) (*http.Response, error) {
				body := ""
				status := http.StatusOK
				if r.URL.Path == "/account" {
					body = tc.account
					if tc.status != 0 {
						status = tc.status
					}
				} else if r.URL.Path == "/api/config" {
					body = tc.config
				}
				if body != "" {
					return &http.Response{StatusCode: status, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body))}, nil
				}
				return base.RoundTrip(r)
			})
			p, err := New(context.Background(), cfg)
			if tc.wantErr != nil {
				if !errors.Is(err, tc.wantErr) || !strings.Contains(err.Error(), tc.step) {
					t.Fatalf("connection error = %v, want %v at %s", err, tc.wantErr, tc.step)
				}
				if strings.Contains(err.Error(), "secret-cookie") || strings.Contains(err.Error(), "https://signed") {
					t.Fatal("connection error leaked response data")
				}
				if p != nil {
					p.Close()
					t.Fatal("failed connection returned a provider")
				}
				if _, err := os.Stat(cfg.CookieFile); !os.IsNotExist(err) {
					t.Fatal("failed connection persisted credentials")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			defer p.Close()
			if id, err := p.AccountID(context.Background()); err != nil || id != tc.wantID {
				t.Fatalf("account ID = %q, %v; want %q", id, err, tc.wantID)
			}
			if _, err := os.Stat(cfg.CookieFile); err != nil {
				t.Fatalf("credentials not persisted: %v", err)
			}
		})
	}
}

func TestConfigConnectionErrorsIncludeSafeDiagnostics(t *testing.T) {
	for _, tc := range []struct {
		name, body, reason string
		status             int
		want               error
	}{
		{name: "gateway HTML", status: 200, body: "<html>secret-cookie https://signed</html>", reason: "invalid response JSON", want: cloudstorage.ErrProtocol},
		{name: "wrong envelope types", status: 200, body: `{"code":"secret-cookie https://signed"}`, reason: "envelope field types", want: cloudstorage.ErrProtocol},
		{name: "missing code", status: 200, body: `{"status":200}`, reason: "missing response code", want: cloudstorage.ErrProtocol},
		{name: "missing status", status: 200, body: `{"code":0}`, reason: "missing response status", want: cloudstorage.ErrProtocol},
		{name: "unauthorized without envelope", status: 401, body: `{}`, want: cloudstorage.ErrAuthentication},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFake(t)
			cfg := f.cfg(t)
			cfg.HTTPClient.Transport = roundTripFunc(func(r *http.Request) (*http.Response, error) {
				if r.URL.Path != "/api/config" {
					t.Errorf("unexpected connection endpoint %s", r.URL.Path)
				}
				return &http.Response{StatusCode: tc.status, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(tc.body))}, nil
			})
			p, err := New(t.Context(), cfg)
			if p != nil {
				p.Close()
				t.Fatal("rejected config returned provider")
			}
			if !errors.Is(err, tc.want) || !strings.Contains(err.Error(), "refresh cookies") {
				t.Fatalf("error = %v, want %v", err, tc.want)
			}
			var detail *cloudstorage.Error
			if !errors.As(err, &detail) || detail.HTTPStatus != tc.status {
				t.Fatalf("missing HTTP status: %v", err)
			}
			if !strings.Contains(err.Error(), tc.reason) || strings.Contains(err.Error(), "secret-cookie") || strings.Contains(err.Error(), "https://signed") {
				t.Fatalf("unsafe or insufficient diagnostics: %v", err)
			}
			if _, err := os.Stat(cfg.CookieFile); !os.IsNotExist(err) {
				t.Fatal("rejected config persisted credentials")
			}
		})
	}
}

func TestDefaultConnectionUsesDriveConfigOnly(t *testing.T) {
	t.Setenv("QUARK_COOKIES", "")
	t.Setenv("QUARK_COOKIE", "")
	for _, uid := range []string{"", "123456789"} {
		name := "bootstrap identity"
		if uid != "" {
			name = "cookie uid"
		}
		t.Run(name, func(t *testing.T) {
			f := newFake(t)
			cfg := f.cfg(t)
			cfg.AccountURL = ""
			if uid != "" {
				cfg.Cookies += "; __uid=" + uid
			}
			base := fakeTransport{f}
			requests := 0
			cfg.HTTPClient.Transport = roundTripFunc(func(r *http.Request) (*http.Response, error) {
				requests++
				if r.URL.Path != "/api/config" {
					t.Errorf("unexpected initialization request: %s", r.URL.Path)
					return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader("account service unavailable")), Header: make(http.Header)}, nil
				}
				if r.URL.Query().Get("pr") != "ucpro" || r.URL.Query().Get("fr") != "pc" {
					t.Error("missing reference driver's query parameters")
				}
				if r.Header.Get("Referer") != "https://pan.quark.cn" || r.Header.Get("Accept") != "application/json, text/plain, */*" || r.Header.Get("User-Agent") != "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36" {
					t.Error("control request headers differ from the reference driver")
				}
				return base.RoundTrip(r)
			})
			p, err := New(t.Context(), cfg)
			if err != nil {
				t.Fatal(err)
			}
			id, err := p.AccountID(t.Context())
			if err != nil || id == "" {
				t.Fatalf("identity: %q %v", id, err)
			}
			if uid != "" && id != uid {
				t.Fatal("cookie uid was not used")
			}
			if strings.Contains(id, "bootstrap") || strings.Contains(id, "old") {
				t.Fatal("identity exposes credentials")
			}
			p.Close()
			// No bootstrap environment or Config cookies on reconnect: load the
			// refreshed cookie and stable checkpoint identity from disk.
			cfg.Cookies = ""
			p, err = New(t.Context(), cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer p.Close()
			if restored, err := p.AccountID(t.Context()); err != nil || restored != id {
				t.Fatalf("refresh changed identity: %q -> %q (%v)", id, restored, err)
			}
			if requests != 2 {
				t.Fatalf("requests = %d, want two config requests", requests)
			}
			p.Close()
			cfg.Cookies = "__pus=different-bootstrap; __puus=old"
			if uid != "" {
				cfg.Cookies += "; __uid=" + uid
			}
			p, err = New(t.Context(), cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer p.Close()
			next, err := p.AccountID(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			if (uid == "" && next == id) || (uid != "" && next != id) {
				t.Fatal("new credentials did not select the expected checkpoint namespace")
			}
		})
	}
}
