package quark

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	cloudstorage "github.com/Niexiawei/simple-cloud-storage"
)

func TestPendingWriterAbortClearsRemoteAndLocalState(t *testing.T) {
	f := newFake(t)
	cfg := f.cfg(t)
	c := connectFake(t, f, cfg)
	f.failPart = 2
	size := int64(10)
	w, err := c.NewWriter(context.Background(), "a", cloudstorage.WriterOptions{Size: &size, Resume: true, ResumeID: "writer"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.WriteString(w, "abcdefghij"); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); !errors.Is(err, cloudstorage.ErrTemporary) {
		t.Fatal(err)
	}
	source := w.(cloudstorage.PendingUpload).ResumeSource()
	if data, err := os.ReadFile(source); err != nil || string(data) != "abcdefghij" {
		t.Fatal("complete recoverable source was not retained")
	}
	if err := w.Abort(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(source); !os.IsNotExist(err) {
		t.Fatal("aborted source remains")
	}
	entries, err := os.ReadDir(filepath.Join(cfg.StateDir, "checkpoints"))
	if err != nil || len(entries) != 0 {
		t.Fatalf("checkpoint remains: %v %v", entries, err)
	}
	f.mu.Lock()
	remaining := len(f.uploads)
	f.mu.Unlock()
	if remaining != 0 {
		t.Fatal("remote multipart was not aborted")
	}
}

type delayedBody struct {
	io.ReadCloser
	wait bool
}

func (b *delayedBody) Read(p []byte) (int, error) {
	if !b.wait {
		b.wait = true
		time.Sleep(40 * time.Second)
	}
	return b.ReadCloser.Read(p)
}

func TestLongDownloadIsNotCutOffByControlTimeout(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newFake(t)
		cfg := f.cfg(t)
		base := fakeTransport{f}
		delay := false
		cfg.HTTPClient = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			response, err := base.RoundTrip(r)
			if err == nil && delay && strings.HasPrefix(r.URL.Path, "/blob/") {
				response.Body = &delayedBody{ReadCloser: response.Body}
			}
			return response, err
		})}
		c := connectFake(t, f, cfg)
		if _, err := c.Upload(context.Background(), "a", strings.NewReader("abcd"), cloudstorage.UploadOptions{}); err != nil {
			t.Fatal(err)
		}
		delay = true
		r, err := c.Open(context.Background(), "a", cloudstorage.ReadOptions{})
		if err != nil {
			t.Fatal(err)
		}
		data, err := io.ReadAll(r)
		r.Close()
		if err != nil || string(data) != "abcd" {
			t.Fatalf("long download truncated: %q %v", data, err)
		}
	})
}

func TestPartRetryHonorsRetryAfter(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newFake(t)
		cfg := f.cfg(t)
		base := fakeTransport{f}
		failed := false
		cfg.HTTPClient = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			if r.Method == http.MethodPut && !failed {
				failed = true
				return &http.Response{StatusCode: 429, Header: http.Header{"Retry-After": {"2"}}, Body: io.NopCloser(strings.NewReader(""))}, nil
			}
			return base.RoundTrip(r)
		})}
		c := connectFake(t, f, cfg)
		start := time.Now()
		if _, err := c.Upload(context.Background(), "a", strings.NewReader("abcd"), cloudstorage.UploadOptions{}); err != nil {
			t.Fatal(err)
		}
		if time.Since(start) != 2*time.Second {
			t.Fatalf("Retry-After was ignored: %v", time.Since(start))
		}
	})
}

func TestConcurrentDirectoryCreationUsesOneID(t *testing.T) {
	f := newFake(t)
	cfg := f.cfg(t)
	c := connectFake(t, f, cfg)
	var wg sync.WaitGroup
	ids := make(chan string, 8)
	for i := 0; i < 8; i++ {
		wg.Go(func() {
			id, err := c.Provider.directory(context.Background(), "apps/test/shared", true)
			if err != nil {
				t.Error(err)
				return
			}
			ids <- id
		})
	}
	wg.Wait()
	close(ids)
	var want string
	for id := range ids {
		if want == "" {
			want = id
		}
		if id != want {
			t.Fatal("concurrent creation returned different directories")
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.mkdirCalls != 3 {
		t.Fatalf("duplicate directories: mkdir=%d", f.mkdirCalls)
	}
}

func TestExpiredSignatureGetsFreshAuthorization(t *testing.T) {
	f := newFake(t)
	cfg := f.cfg(t)
	base := fakeTransport{f}
	failed := false
	auths := 0
	cfg.HTTPClient = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.URL.Path == "/api/file/upload/auth" {
			auths++
		}
		if r.Method == http.MethodPut && !failed {
			failed = true
			return &http.Response{StatusCode: 403, Header: make(http.Header), Body: io.NopCloser(strings.NewReader("<Error><Code>SignatureDoesNotMatch</Code></Error>"))}, nil
		}
		return base.RoundTrip(r)
	})}
	c := connectFake(t, f, cfg)
	if _, err := c.Upload(context.Background(), "a", strings.NewReader("abcd"), cloudstorage.UploadOptions{}); err != nil {
		t.Fatal(err)
	}
	if !failed || auths != 3 {
		t.Fatalf("expected two part signatures plus Commit: failed=%v auths=%d", failed, auths)
	}
}

func TestResumeRecreatesOnlyDefinitelyExpiredSession(t *testing.T) {
	f := newFake(t)
	cfg := f.cfg(t)
	c := connectFake(t, f, cfg)
	f.failPart = 2
	if _, err := c.Upload(context.Background(), "a", strings.NewReader("abcdefghij"), cloudstorage.UploadOptions{Resume: true}); err == nil {
		t.Fatal("expected interruption")
	}
	f.mu.Lock()
	delete(f.uploads, "task-1")
	delete(f.files, "file-task-1")
	f.failPart = 0
	f.mu.Unlock()
	if _, err := c.Upload(context.Background(), "a", strings.NewReader("abcdefghij"), cloudstorage.UploadOptions{Resume: true}); err != nil {
		t.Fatal(err)
	}
	if f.preCalls != 2 {
		t.Fatalf("expected one bounded replacement: pre=%d", f.preCalls)
	}
}

func TestLostCommitResponseRecoversOriginalTask(t *testing.T) {
	f := newFake(t)
	cfg := f.cfg(t)
	base := fakeTransport{f}
	lost := false
	cfg.HTTPClient = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		response, err := base.RoundTrip(r)
		if err == nil && !lost && r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/object/") {
			lost = true
			response.Body.Close()
			return nil, io.ErrUnexpectedEOF
		}
		return response, err
	})}
	c := connectFake(t, f, cfg)
	if _, err := c.Upload(context.Background(), "a", strings.NewReader("abcd"), cloudstorage.UploadOptions{Resume: true}); !errors.Is(err, cloudstorage.ErrTemporary) {
		t.Fatalf("lost Commit = %v", err)
	}
	if _, err := c.Upload(context.Background(), "a", strings.NewReader("abcd"), cloudstorage.UploadOptions{Resume: true}); err != nil {
		t.Fatal(err)
	}
	if f.preCalls != 1 || f.partCalls[1] != 1 || f.finishCalls != 1 {
		t.Fatalf("original task was not reused: pre=%d part=%d finish=%d", f.preCalls, f.partCalls[1], f.finishCalls)
	}
}

func TestCommittedUploadResumesFinishWithoutRepeatingHash(t *testing.T) {
	f := newFake(t)
	cfg := f.cfg(t)
	base := fakeTransport{f}
	rejectFinish, rejectHash := true, false
	cfg.HTTPClient = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if (rejectFinish && r.URL.Path == "/api/file/upload/finish") || (rejectHash && r.URL.Path == "/api/file/update/hash") {
			return &http.Response{StatusCode: 503, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{"status":503,"code":1}`))}, nil
		}
		return base.RoundTrip(r)
	})}
	c := connectFake(t, f, cfg)
	if _, err := c.Upload(context.Background(), "a", strings.NewReader("abcd"), cloudstorage.UploadOptions{Resume: true}); !errors.Is(err, cloudstorage.ErrTemporary) {
		t.Fatalf("Finish failure=%v", err)
	}
	rejectFinish, rejectHash = false, true
	if _, err := c.Upload(context.Background(), "a", strings.NewReader("abcd"), cloudstorage.UploadOptions{Resume: true}); err != nil {
		t.Fatal(err)
	}
	if f.preCalls != 1 || f.partCalls[1] != 1 || f.finishCalls != 1 {
		t.Fatal("committed upload was restarted")
	}
}

func TestChangedSourceCannotReuseUpload(t *testing.T) {
	f := newFake(t)
	cfg := f.cfg(t)
	c := connectFake(t, f, cfg)
	f.failPart = 2
	if _, err := c.Upload(context.Background(), "a", strings.NewReader("abcdefghij"), cloudstorage.UploadOptions{Resume: true}); !errors.Is(err, cloudstorage.ErrTemporary) {
		t.Fatal(err)
	}
	if _, err := c.Upload(context.Background(), "a", strings.NewReader("abcdefghiK"), cloudstorage.UploadOptions{Resume: true}); !errors.Is(err, cloudstorage.ErrInvalidCheckpoint) {
		t.Fatalf("changed source accepted: %v", err)
	}
	if f.preCalls != 1 {
		t.Fatal("changed source created a replacement task")
	}
}

type failingSecretDelete struct {
	cloudstorage.TokenStore
	fail bool
}

func (s *failingSecretDelete) Delete(ctx context.Context, key string) error {
	if s.fail {
		return os.ErrPermission
	}
	return s.TokenStore.Delete(ctx, key)
}

func TestCleanupFailureRetainsConfirmationCheckpoint(t *testing.T) {
	f := newFake(t)
	cfg := f.cfg(t)
	store, err := cloudstorage.NewFileTokenStore(cfg.StateDir)
	if err != nil {
		t.Fatal(err)
	}
	secrets := &failingSecretDelete{TokenStore: store, fail: true}
	cfg.TokenStore = secrets
	c := connectFake(t, f, cfg)
	for attempt := 0; attempt < 2; attempt++ {
		if _, err := c.Upload(context.Background(), "a", strings.NewReader("abcd"), cloudstorage.UploadOptions{Resume: true}); !errors.Is(err, os.ErrPermission) {
			t.Fatalf("cleanup failure=%v", err)
		}
		entries, err := os.ReadDir(filepath.Join(cfg.StateDir, "checkpoints"))
		if err != nil || len(entries) != 1 {
			t.Fatal("cleanup failure lost confirmation checkpoint")
		}
	}
	secrets.fail = false
	if _, err := c.Upload(context.Background(), "a", strings.NewReader("abcd"), cloudstorage.UploadOptions{Resume: true}); err != nil {
		t.Fatal(err)
	}
	if f.preCalls != 1 || f.partCalls[1] != 1 {
		t.Fatal("cleanup retry created or uploaded a duplicate")
	}
}

func TestChangedDownloadObjectOrDestinationPreservesPartial(t *testing.T) {
	f := newFake(t)
	cfg := f.cfg(t)
	c := connectFake(t, f, cfg)
	info, err := c.Upload(context.Background(), "a", strings.NewReader("abcdefghij"), cloudstorage.UploadOptions{})
	if err != nil {
		t.Fatal(err)
	}
	dst := filepath.Join(t.TempDir(), "file")
	f.breakDownload = true
	if err := c.DownloadToFile(context.Background(), "a", dst, cloudstorage.DownloadOptions{Resume: true}); err == nil {
		t.Fatal("interruption not detected")
	}
	if err := c.DownloadToFile(context.Background(), "a", dst+"other", cloudstorage.DownloadOptions{Resume: true}); !errors.Is(err, cloudstorage.ErrInvalidCheckpoint) {
		t.Fatalf("changed destination accepted: %v", err)
	}
	f.mu.Lock()
	item := f.files[info.ID]
	item.UpdatedAt++
	f.files[info.ID] = item
	f.mu.Unlock()
	if err := c.DownloadToFile(context.Background(), "a", dst, cloudstorage.DownloadOptions{Resume: true}); !errors.Is(err, cloudstorage.ErrInvalidCheckpoint) {
		t.Fatalf("changed cloud version accepted: %v", err)
	}
	if data, err := os.ReadFile(dst + ".part"); err != nil || string(data) != "abc" {
		t.Fatal("invalid resume modified partial data")
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (fn roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return fn(r) }

func TestPREIsNotReplayedAfterLostResponseOrRedirect(t *testing.T) {
	for _, mode := range []string{"lost response", "redirect"} {
		t.Run(mode, func(t *testing.T) {
			f := newFake(t)
			cfg := f.cfg(t)
			attempts := 0
			base := fakeTransport{f}
			cfg.HTTPClient = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
				if r.URL.Path != "/api/file/upload/pre" {
					return base.RoundTrip(r)
				}
				attempts++
				response, err := base.RoundTrip(r)
				if err != nil {
					return nil, err
				}
				if mode == "lost response" {
					response.Body.Close()
					return nil, io.ErrUnexpectedEOF
				}
				response.StatusCode = 307
				response.Header.Set("Location", f.server.URL+"/redirected-pre")
				return response, nil
			})}
			c := connectFake(t, f, cfg)
			if _, err := c.Upload(context.Background(), "a", strings.NewReader("abcd"), cloudstorage.UploadOptions{}); err == nil {
				t.Fatal("ambiguous PRE reported success")
			}
			if attempts != 1 || f.preCalls != 1 {
				t.Fatalf("PRE replayed: attempts=%d allocations=%d", attempts, f.preCalls)
			}
		})
	}
}

func TestCookieOutOfOrderMergeAndFailedPersistence(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cookies.json")
	var state cookieState
	if err := state.load(path, "__pus=bootstrap; __puus=old"); err != nil {
		t.Fatal(err)
	}
	if err := state.activate(); err != nil {
		t.Fatal(err)
	}
	before := state.snapshot()
	if err := state.merge([]*http.Cookie{{Name: "__puus", Value: "new"}}, before); err != nil {
		t.Fatal(err)
	}
	if err := state.merge([]*http.Cookie{{Name: "__puus", Value: "stale"}}, before); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(state.snapshot(), "stale") {
		t.Fatal("late response rolled credentials back")
	}
	oldBytes, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	blocker := filepath.Join(filepath.Dir(path), "not-a-directory")
	if err := os.WriteFile(blocker, []byte("file"), 0600); err != nil {
		t.Fatal(err)
	}
	state.path = filepath.Join(blocker, "cookies.json")
	before = state.snapshot()
	if err := state.merge([]*http.Cookie{{Name: "__puus", Value: "cannot-persist"}}, before); err == nil {
		t.Fatal("persistence failure was ignored")
	}
	if state.snapshot() != before {
		t.Fatal("failed persistence committed a new in-memory credential")
	}
	if bytes, err := os.ReadFile(path); err != nil || string(bytes) != string(oldBytes) {
		t.Fatal("old persisted credentials were damaged")
	}
}

func TestTransportErrorsDoNotExposeSignedURLs(t *testing.T) {
	sensitive := &url.Error{Op: "Get", URL: "https://download.quark.cn/file?token=secret-signature", Err: io.ErrUnexpectedEOF}
	err := safeTransport(context.Background(), sensitive)
	if !errors.Is(err, cloudstorage.ErrTemporary) || strings.Contains(err.Error(), "secret-signature") {
		t.Fatal("transport error leaked a signed URL")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if !errors.Is(safeTransport(ctx, sensitive), context.Canceled) {
		t.Fatal("lost cancellation identity")
	}
}

func TestEmptyFileRoundTrip(t *testing.T) {
	f := newFake(t)
	cfg := f.cfg(t)
	c := connectFake(t, f, cfg)
	info, err := c.Upload(context.Background(), "empty", strings.NewReader(""), cloudstorage.UploadOptions{Resume: true})
	if err != nil || info.Size != 0 {
		t.Fatalf("empty upload=%+v %v", info, err)
	}
	dst := filepath.Join(t.TempDir(), "empty")
	if err := c.DownloadToFile(context.Background(), "empty", dst, cloudstorage.DownloadOptions{Resume: true}); err != nil {
		t.Fatal(err)
	}
	if stat, err := os.Stat(dst); err != nil || stat.Size() != 0 {
		t.Fatal("empty download failed")
	}
}

func TestHashInstantUploadSkipsPartsAndFinish(t *testing.T) {
	f := newFake(t)
	cfg := f.cfg(t)
	c := connectFake(t, f, cfg)
	f.instant = true
	info, err := c.Upload(context.Background(), "a", strings.NewReader("abcd"), cloudstorage.UploadOptions{Resume: true})
	if err != nil || info.Size != 4 {
		t.Fatalf("instant upload=%+v %v", info, err)
	}
	if len(f.partCalls) != 0 || f.finishCalls != 0 {
		t.Fatal("instant upload sent multipart body or Finish")
	}
}

func TestDownloadRedirectKeepsSnapshotOnlyOnTrustedHosts(t *testing.T) {
	for _, trusted := range []bool{true, false} {
		name := "trusted"
		if !trusted {
			name = "untrusted"
		}
		t.Run(name, func(t *testing.T) {
			f := newFake(t)
			cfg := f.cfg(t)
			base := fakeTransport{f}
			redirect := false
			cfg.HTTPClient = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
				if r.URL.Host != "127.0.0.1:9876" {
					t.Error("credentials reached an untrusted host")
				}
				if redirect && strings.HasPrefix(r.URL.Path, "/blob/") && r.URL.Query().Get("edge") == "" {
					location := r.URL.String() + "?edge=1"
					if !trusted {
						location = "https://untrusted.example/steal"
					}
					return &http.Response{StatusCode: 302, Header: http.Header{"Location": {location}}, Body: io.NopCloser(strings.NewReader(""))}, nil
				}
				return base.RoundTrip(r)
			})}
			c := connectFake(t, f, cfg)
			if _, err := c.Upload(context.Background(), "a", strings.NewReader("abcd"), cloudstorage.UploadOptions{}); err != nil {
				t.Fatal(err)
			}
			redirect = true
			r, err := c.Open(context.Background(), "a", cloudstorage.ReadOptions{})
			if !trusted {
				if !errors.Is(err, cloudstorage.ErrProtocol) {
					t.Fatalf("untrusted redirect accepted: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			data, err := io.ReadAll(r)
			r.Close()
			if err != nil || string(data) != "abcd" {
				t.Fatalf("trusted redirect=%q %v", data, err)
			}
		})
	}
}

func TestProviderMessagesAreSanitized(t *testing.T) {
	status, code := 400, 123
	err := statusError(400, &envelope{Status: &status, Code: &code, Message: "secret-cookie https://signed"}, "")
	if strings.Contains(err.Error(), "secret-cookie") || strings.Contains(err.Error(), "https") {
		t.Fatal("provider message leaked secrets")
	}
	if _, err := parseCookies("__pus=x\r\nInjected: yes"); err == nil {
		t.Fatal("header injection accepted")
	}
}
