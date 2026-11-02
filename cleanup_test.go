package cloudstorage

import (
	"context"
	"errors"
	"os"
	"testing"
	"testing/synctest"
	"time"
)

func TestCleanupTimeoutAndRetry(t *testing.T) {
	for _, mode := range []string{"streaming", "spooled", "copy failure", "explicit abort", "caller cancelled"} {
		t.Run(mode, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				p := newMockProvider()
				p.completeErr = ErrTemporary
				type contextKey struct{}
				p.abortHook = func(ctx context.Context) error {
					if ctx.Value(contextKey{}) != "value" {
						t.Error("cleanup lost context values")
					}
					<-ctx.Done()
					return ctx.Err()
				}
				dir := t.TempDir()
				c, err := NewClient(p, Config{Prefix: "app", PartSize: 4, MultipartThreshold: 4, TempDir: dir, CleanupTimeout: time.Second})
				if err != nil {
					t.Fatal(err)
				}
				ctx, cancel := context.WithCancel(context.WithValue(context.Background(), contextKey{}, "value"))
				defer cancel()
				start := time.Now()
				if mode == "copy failure" {
					_, err = c.Upload(ctx, "a", &failingSource{}, UploadOptions{Size: sizePointer(8)})
					if !errors.Is(err, ErrPermission) || !errors.Is(err, context.DeadlineExceeded) {
						t.Fatalf("Upload = %v", err)
					}
				} else {
					opts := WriterOptions{Size: sizePointer(8)}
					if mode == "spooled" {
						opts.Size = nil
					}
					w, createErr := c.NewWriter(ctx, "a", opts)
					if createErr != nil {
						t.Fatal(createErr)
					}
					if _, err := w.Write([]byte("abcdefgh")); err != nil {
						t.Fatal(err)
					}
					switch mode {
					case "explicit abort":
						err = w.Abort(ctx)
					case "caller cancelled":
						cancel()
						err = w.Abort(ctx)
						if !errors.Is(err, context.Canceled) {
							t.Fatal(err)
						}
					default:
						if mode == "streaming" {
							cancel()
						}
						err = w.Close()
						if mode == "streaming" && !errors.Is(err, context.Canceled) {
							t.Fatal(err)
						}
						if mode == "spooled" && !errors.Is(err, ErrTemporary) {
							t.Fatal(err)
						}
					}
					if mode != "caller cancelled" && !errors.Is(err, context.DeadlineExceeded) {
						t.Fatalf("cleanup error = %v", err)
					}
					if mode != "explicit abort" && mode != "caller cancelled" {
						if again := w.Close(); again != err {
							t.Fatalf("Close error not sticky: %v != %v", again, err)
						}
					}
					p.abortHook = nil
					if err := w.Abort(context.WithoutCancel(ctx)); err != nil {
						t.Fatal(err)
					}
					if !p.sessions[0].aborted {
						t.Fatal("retry did not abort session")
					}
				}
				elapsed := time.Since(start)
				want := time.Second
				if mode == "caller cancelled" {
					want = 0
				}
				if elapsed != want {
					t.Fatalf("cleanup took %v, want %v", elapsed, want)
				}
				entries, err := os.ReadDir(dir)
				if err != nil || len(entries) != 0 {
					t.Fatalf("spool remains: %v %v", entries, err)
				}
			})
		})
	}
}

type failingSource struct{}

func (*failingSource) Read([]byte) (int, error) { return 0, ErrPermission }

func TestCleanupConfiguration(t *testing.T) {
	if _, err := NewClient(newMockProvider(), Config{Prefix: "app", CleanupTimeout: -time.Second}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatal(err)
	}
	c, err := NewClient(newMockProvider(), Config{Prefix: "app"})
	if err != nil {
		t.Fatal(err)
	}
	if c.config.CleanupTimeout != 5*time.Second {
		t.Fatal("wrong default cleanup timeout")
	}
}

func TestAbortEarlierDeadlineStillReleasesLocalResources(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := newMockProvider()
		p.abortHook = func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() }
		c := testClient(t, p)
		w, err := c.NewWriter(context.Background(), "a", WriterOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := w.Write([]byte("data")); err != nil {
			t.Fatal(err)
		}
		// Arrange both local and remote resources to exercise cleanup ordering.
		session, err := p.CreateMultipart(context.Background(), "apps/test/a", UploadOptions{})
		if err != nil {
			t.Fatal(err)
		}
		w.(*uploadWriter).session = session
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		start := time.Now()
		if err := w.Abort(ctx); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatal(err)
		}
		if time.Since(start) != time.Second {
			t.Fatal("caller deadline was extended")
		}
		entries, err := os.ReadDir(c.config.TempDir)
		if err != nil || len(entries) != 0 {
			t.Fatalf("local resources remain after remote timeout: %v %v", entries, err)
		}
		p.abortHook = nil
		if err := w.Abort(context.Background()); err != nil {
			t.Fatal(err)
		}
	})
}
