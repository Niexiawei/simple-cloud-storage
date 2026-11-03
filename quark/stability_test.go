package quark

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	cloudstorage "github.com/Niexiawei/simple-cloud-storage"
)

type failPartCheckpoint struct {
	cloudstorage.CheckpointStore
	saves int
	fail  bool
}

func (s *failPartCheckpoint) Save(ctx context.Context, cp cloudstorage.Checkpoint) error {
	s.saves++
	if s.fail && s.saves == 2 {
		return cloudstorage.ErrPermission
	}
	return s.CheckpointStore.Save(ctx, cp)
}

func TestUnconfirmedPartIsReplayedAfterCheckpointWriteFailure(t *testing.T) {
	f := newFake(t)
	cfg := f.cfg(t)
	store, err := cloudstorage.NewFileCheckpointStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	fault := &failPartCheckpoint{CheckpointStore: store, fail: true}
	storageCfg := cloudstorage.Config{Prefix: "apps/test", TempDir: t.TempDir(), CheckpointStore: fault}
	c, err := Connect(context.Background(), cfg, storageCfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Upload(context.Background(), "a", strings.NewReader("abcdefghij"), cloudstorage.UploadOptions{Resume: true}); !errors.Is(err, cloudstorage.ErrPermission) {
		t.Fatalf("checkpoint failure was not returned: %v", err)
	}
	c.Close()
	fault.fail = false
	c, err = Connect(context.Background(), cfg, storageCfg)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if _, err := c.Upload(context.Background(), "a", strings.NewReader("abcdefghij"), cloudstorage.UploadOptions{Resume: true}); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.preCalls != 1 || f.partCalls[1] != 2 || string(f.data["file-task-1"]) != "abcdefghij" {
		t.Fatal("unconfirmed part was skipped or an extra session was created")
	}
}

type failingDownloadBody struct {
	readErr, closeErr error
}

func (b failingDownloadBody) Read([]byte) (int, error) { return 0, b.readErr }
func (b failingDownloadBody) Close() error             { return b.closeErr }

func TestDownloadErrorsAreStickyAndRedacted(t *testing.T) {
	t.Parallel()
	secret := &url.Error{Op: "Get", URL: "https://dl.quark.cn/file?token=private-signature", Err: io.ErrUnexpectedEOF}
	for _, name := range []string{"tail", "body", "short", "close"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			body := failingDownloadBody{readErr: secret}
			remaining := int64(0)
			if name == "body" || name == "short" {
				remaining = 4
			}
			if name == "short" {
				body.readErr = io.EOF
			}
			if name == "close" {
				body.closeErr = secret
			}
			r := &validatedReader{body: body, ctx: ctx, cancel: cancel, remaining: remaining}
			var err error
			if name == "close" {
				err = r.Close()
			} else {
				_, err = r.Read(make([]byte, 8))
				_, again := r.Read(make([]byte, 8))
				if again != err {
					t.Fatal("terminal read error changed")
				}
			}
			want := cloudstorage.ErrTemporary
			if name == "short" {
				want = io.ErrUnexpectedEOF
			}
			if !errors.Is(err, want) || strings.Contains(err.Error(), "private-signature") {
				t.Fatalf("download error = %v", err)
			}
		})
	}
}

func TestAutoRefreshRetriesAndJoinsOnCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newFake(t)
		cfg := f.cfg(t)
		cfg.DisableAutoRefresh = false
		cfg.RefreshInterval = 100 * time.Minute
		var mu sync.Mutex
		calls := 0
		cfg.HTTPClient = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			if r.URL.Path == "/api/config" {
				mu.Lock()
				calls++
				n := calls
				mu.Unlock()
				if n == 2 {
					return nil, io.ErrUnexpectedEOF
				}
			}
			return fakeTransport{f}.RoundTrip(r)
		})}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		p, err := New(ctx, cfg)
		if err != nil {
			t.Fatal(err)
		}
		defer p.Close()
		synctest.Wait()
		// The first background request fails transiently and its safe GET retry succeeds.
		time.Sleep(100*time.Minute + time.Second)
		synctest.Wait()
		if err := p.LastRefreshError(); err != nil {
			t.Fatal(err)
		}
		cancel()
		synctest.Wait()
		if err := p.Close(); err != nil {
			t.Fatal(err)
		}
		select {
		case <-p.done:
		default:
			t.Fatal("Close returned before refresh goroutine exited")
		}
		mu.Lock()
		before := calls
		mu.Unlock()
		time.Sleep(200 * time.Minute)
		synctest.Wait()
		mu.Lock()
		defer mu.Unlock()
		if before != calls || before < 3 {
			t.Fatalf("refresh requests continued after cancellation: %d -> %d", before, calls)
		}
	})
}
