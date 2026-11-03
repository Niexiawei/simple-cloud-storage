package cloudstorage

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestSpoolSizeLimit(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"known", "unknown", "override", "exact", "negative"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			p := newMockProvider()
			p.caps.RequiresFullHash = true
			dir := t.TempDir()
			c, err := NewClient(p, Config{Prefix: "app", TempDir: dir, MaxTempFileSize: 4})
			if err != nil {
				t.Fatal(err)
			}
			opts := WriterOptions{}
			switch name {
			case "known":
				opts.Size = sizePointer(5)
			case "override":
				opts.MaxTempFileSize = 8
			case "negative":
				opts.MaxTempFileSize = -1
			}
			w, err := c.NewWriter(context.Background(), "a", opts)
			if name == "known" || name == "negative" {
				want := ErrTempLimit
				if name == "negative" {
					want = ErrInvalidArgument
				}
				if !errors.Is(err, want) {
					t.Fatalf("NewWriter = %v, want %v", err, want)
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				if n, err := io.WriteString(w, "abcd"); n != 4 || err != nil {
					t.Fatalf("Write = %d, %v", n, err)
				}
				if name == "unknown" {
					if n, err := io.WriteString(w, "e"); n != 0 || !errors.Is(err, ErrTempLimit) {
						t.Fatalf("overflow Write = %d, %v", n, err)
					}
					if err := w.Close(); !errors.Is(err, ErrTempLimit) {
						t.Fatal(err)
					}
				} else {
					if name == "override" {
						if _, err := io.WriteString(w, "efgh"); err != nil {
							t.Fatal(err)
						}
					}
					if err := w.Close(); err != nil {
						t.Fatal(err)
					}
				}
			}
			if name != "exact" && name != "override" && (len(p.sessions) != 0 || len(p.uploads) != 0) {
				t.Fatal("failed validation performed remote I/O")
			}
			entries, err := os.ReadDir(dir)
			if err != nil || len(entries) != 0 {
				t.Fatalf("temporary file leaked: %v %v", entries, err)
			}
		})
	}
}

func TestPartSizeBoundAndTinyMultipartAllocation(t *testing.T) {
	t.Parallel()
	if _, err := NewClient(newMockProvider(), Config{Prefix: "app", PartSize: MaxPartSize + 1}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatal(err)
	}
	c := testClient(t, newMockProvider())
	if _, err := c.NewWriter(context.Background(), "a", WriterOptions{PartSize: MaxPartSize + 1}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatal(err)
	}
	w, err := c.NewWriter(context.Background(), "a", WriterOptions{Size: sizePointer(4), PartSize: MaxPartSize, MultipartThreshold: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Abort(context.Background())
	if got := cap(w.(*uploadWriter).partBuf); got != 4 {
		t.Fatalf("tiny object allocated %d bytes of multipart buffer", got)
	}
	if _, err := io.WriteString(w, "abcd"); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
}

func oldSpool(t *testing.T, dir, name string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte("data"), 0600); err != nil {
		t.Fatal(err)
	}
	old := time.Now().Add(-48 * time.Hour)
	if err := os.Chtimes(path, old, old); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestCleanupTempUploadsPreservesPendingAndActive(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dir := t.TempDir()
	orphan := oldSpool(t, dir, ".simple-cloud-storage-orphan.upload")
	pending := oldSpool(t, dir, ".simple-cloud-storage-pending.upload")
	keep := oldSpool(t, dir, ".simple-cloud-storage-explicit.upload")
	other := oldSpool(t, dir, "user.upload")
	fresh := filepath.Join(dir, ".simple-cloud-storage-fresh.upload")
	if err := os.WriteFile(fresh, nil, 0600); err != nil {
		t.Fatal(err)
	}
	nested := filepath.Join(dir, "child")
	if err := os.Mkdir(nested, 0700); err != nil {
		t.Fatal(err)
	}
	child := oldSpool(t, nested, ".simple-cloud-storage-child.upload")
	fakeDir := filepath.Join(dir, ".simple-cloud-storage-directory.upload")
	if err := os.Mkdir(fakeDir, 0700); err != nil {
		t.Fatal(err)
	}
	c, err := NewClient(newMockProvider(), Config{Prefix: "app", TempDir: dir})
	if err != nil {
		t.Fatal(err)
	}
	w, err := c.NewWriter(ctx, "active", WriterOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Abort(ctx)
	active := w.(*uploadWriter).filePath
	old := time.Now().Add(-48 * time.Hour)
	if err := os.Chtimes(active, old, old); err != nil {
		t.Fatal(err)
	}
	store, err := NewFileCheckpointStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	state := UploadState{Size: 4, SHA256: strings.Repeat("0", 64), PartSize: 4, TotalParts: 1, Source: pending}
	data, _ := json.Marshal(state)
	cp, err := c.newCheckpoint(ctx, "pending", CheckpointUpload, "resume", data)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Save(ctx, cp); err != nil {
		t.Fatal(err)
	}
	opts := TempCleanupOptions{MinAge: 24 * time.Hour, DryRun: true, Keep: []string{keep}, Checkpoints: store}
	files, err := CleanupTempUploads(ctx, dir, opts)
	if err != nil || len(files) != 1 || files[0].Path != orphan || files[0].Removed {
		t.Fatalf("dry run = %+v, %v", files, err)
	}
	if _, err := os.Stat(orphan); err != nil {
		t.Fatal("dry run removed a file")
	}
	opts.DryRun = false
	files, err = CleanupTempUploads(ctx, dir, opts)
	if err != nil || len(files) != 1 || !files[0].Removed {
		t.Fatalf("cleanup = %+v, %v", files, err)
	}
	for _, path := range []string{pending, keep, other, fresh, child, fakeDir, active} {
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("protected file %s: %v", path, err)
		}
	}
	if _, err := os.Stat(orphan); !os.IsNotExist(err) {
		t.Fatalf("orphan remains: %v", err)
	}
}

func TestCleanupTempUploadsFailsClosed(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	orphan := oldSpool(t, dir, ".simple-cloud-storage-orphan.upload")
	stateDir := t.TempDir()
	store, err := NewFileCheckpointStore(stateDir)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(stateDir, "checkpoints", "broken.json"), []byte("{"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := CleanupTempUploads(context.Background(), dir, TempCleanupOptions{MinAge: time.Hour, Checkpoints: store}); !errors.Is(err, ErrInvalidCheckpoint) {
		t.Fatal(err)
	}
	if _, err := os.Stat(orphan); err != nil {
		t.Fatal("cleanup deleted data despite an unreadable checkpoint")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := CleanupTempUploads(ctx, dir, TempCleanupOptions{MinAge: time.Hour}); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err := CleanupTempUploads(context.Background(), dir, TempCleanupOptions{}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatal(err)
	}
}

type geometrySession struct {
	MultipartSession
}

func (geometrySession) PartSize() int64 { return MaxPartSize + 1 }

type oversizedGeometryProvider struct{ *mockProvider }

func (p oversizedGeometryProvider) CreateMultipart(ctx context.Context, path string, opts UploadOptions) (MultipartSession, error) {
	s, err := p.mockProvider.CreateMultipart(ctx, path, opts)
	return geometrySession{s}, err
}

func TestProviderCannotForceOversizedBuffer(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"streaming", "spooled"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			p := newMockProvider()
			c, err := NewClient(oversizedGeometryProvider{p}, Config{Prefix: "app", MultipartThreshold: 1, TempDir: t.TempDir()})
			if err != nil {
				t.Fatal(err)
			}
			if name == "streaming" {
				_, err = c.NewWriter(context.Background(), "a", WriterOptions{Size: sizePointer(4)})
			} else {
				_, err = c.Upload(context.Background(), "a", strings.NewReader("abcd"), UploadOptions{})
			}
			if !errors.Is(err, ErrProtocol) || len(p.sessions) != 1 || !p.sessions[0].aborted {
				t.Fatalf("bad geometry was not rejected and aborted: %v", err)
			}
		})
	}
}

func TestCleanupSkipsSymlinks(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	target := oldSpool(t, t.TempDir(), "outside.bin")
	link := filepath.Join(dir, ".simple-cloud-storage-link.upload")
	if err := os.Symlink(target, link); err != nil {
		t.Skipf("symlink creation unavailable: %v", err)
	}
	files, err := CleanupTempUploads(context.Background(), dir, TempCleanupOptions{MinAge: time.Hour})
	if err != nil || len(files) != 0 {
		t.Fatalf("symlink selected for cleanup: %+v %v", files, err)
	}
	if _, err := os.Stat(target); err != nil {
		t.Fatal("cleanup changed a symlink target")
	}
}
