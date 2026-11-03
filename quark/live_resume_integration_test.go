//go:build integration

package quark_test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	cloudstorage "github.com/Niexiawei/simple-cloud-storage"
	"github.com/Niexiawei/simple-cloud-storage/quark"
)

// This helper exits immediately from a progress callback, after the corresponding
// checkpoint has been persisted. It deliberately bypasses all deferred cleanup.
func TestLiveResumeProcessHelper(t *testing.T) {
	mode := os.Getenv("SCS_LIVE_RESUME_MODE")
	if os.Getenv("QUARK_LIVE_RESUME_TEST") != "1" || mode == "" {
		t.Skip("opt-in live subprocess helper")
	}
	dir, prefix := os.Getenv("SCS_LIVE_RESUME_DIR"), os.Getenv("SCS_LIVE_RESUME_PREFIX")
	ctx, cancel := context.WithTimeout(t.Context(), 80*time.Minute)
	defer cancel()
	c, err := quark.Connect(ctx, quark.Config{StateDir: dir}, cloudstorage.Config{Prefix: prefix, TempDir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	first := int64(-1)
	progress := func(p cloudstorage.Progress) {
		if first < 0 {
			first = p.Transferred
		}
		if strings.HasSuffix(mode, "interrupt") && p.Transferred > 0 && p.Transferred < p.Total {
			if strings.HasPrefix(mode, "upload") {
				os.Exit(73)
			}
			os.Exit(74)
		}
	}
	const key = "media/interrupted.bin"
	if strings.HasPrefix(mode, "upload") {
		info, err := c.UploadFile(ctx, key, filepath.Join(dir, "source.bin"), cloudstorage.UploadOptions{
			Resume: true, ResumeID: "phase4-process", Progress: progress,
		})
		if err != nil {
			t.Fatal(err)
		}
		if mode != "upload-resume" || first <= 0 || first >= info.Size {
			t.Fatalf("upload did not resume unfinished parts: first=%d total=%d", first, info.Size)
		}
		data, err := json.Marshal(info)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "result.json"), data, 0600); err != nil {
			t.Fatal(err)
		}
	} else {
		if err := c.DownloadToFile(ctx, key, filepath.Join(dir, "download.bin"), cloudstorage.DownloadOptions{Resume: true, Progress: progress}); err != nil {
			t.Fatal(err)
		}
		if mode != "download-resume" || first <= 0 {
			t.Fatalf("download did not resume a partial file: first=%d", first)
		}
	}
}

// TestLiveQuarkInterruptedResume uses a unique copy of D:\input.mp4 so hash-based
// instant upload cannot bypass the uncompleted multipart session under test.
// It creates one object in an isolated prefix, deletes it only after verification,
// and retains its fixture, state and remote object on failure for diagnosis.
func TestLiveQuarkInterruptedResume(t *testing.T) {
	if os.Getenv("QUARK_LIVE_RESUME_TEST") != "1" {
		t.Skip("set QUARK_LIVE_RESUME_TEST=1 and configure persisted credentials")
	}
	t.Chdir("..")
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		t.Fatal(err)
	}
	runID := os.Getenv("QUARK_TEST_RUN_ID")
	if runID == "" {
		runID = time.Now().UTC().Format("20060102T150405") + "-" + hex.EncodeToString(nonce[:8])
	}
	if runID == "." || runID == ".." || strings.ContainsAny(runID, "/\\:") {
		t.Fatal("QUARK_TEST_RUN_ID must be one directory name")
	}
	base := strings.Trim(strings.TrimSpace(os.Getenv("QUARK_TEST_PREFIX")), "/")
	if base == "" {
		base = "integration/simple-cloud-storage"
	}
	prefix := base + "/phase4-" + runID
	dir, err := filepath.Abs(filepath.Join(".simple-cloud-storage", "phase4-"+runID))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	sourcePath := filepath.Join(dir, "source.bin")
	if _, err := os.Stat(sourcePath); os.IsNotExist(err) {
		src, err := os.Open(`D:\input.mp4`)
		if err != nil {
			t.Fatal(err)
		}
		dst, err := os.OpenFile(sourcePath, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
		if err != nil {
			src.Close()
			t.Fatal(err)
		}
		size, copyErr := io.Copy(dst, src)
		_, writeErr := dst.WriteAt(nonce[:], 0)
		if err := errors.Join(copyErr, writeErr, dst.Sync(), dst.Close(), src.Close()); err != nil {
			t.Fatal(err)
		}
		if size <= cloudstorage.MaxPartSize {
			// The usual fixture is 77 MB and the service returns smaller parts;
			// a single-part response will fail the deliberate-exit assertion below.
			t.Logf("fixture size=%d; service must select multipart geometry", size)
		}
	} else if err != nil {
		t.Fatal(err)
	}
	t.Logf("remote=%s/media/interrupted.bin state=%s", prefix, dir)
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	run := func(mode string, exitCode int) {
		t.Helper()
		ctx, cancel := context.WithTimeout(t.Context(), 85*time.Minute)
		defer cancel()
		cmd := exec.CommandContext(ctx, executable, "-test.run=^TestLiveResumeProcessHelper$", "-test.timeout=80m")
		cmd.Env = append(os.Environ(), "SCS_LIVE_RESUME_MODE="+mode, "SCS_LIVE_RESUME_DIR="+dir, "SCS_LIVE_RESUME_PREFIX="+prefix)
		output, err := cmd.CombinedOutput()
		if exitCode != 0 {
			var exited *exec.ExitError
			if !errors.As(err, &exited) || exited.ExitCode() != exitCode {
				t.Fatalf("%s did not stop after confirmed progress: %v\n%s", mode, err, output)
			}
		} else if err != nil {
			t.Fatalf("%s failed: %v\n%s", mode, err, output)
		}
		t.Logf("%s passed", mode)
	}
	ctx := t.Context()
	store, err := cloudstorage.NewFileCheckpointStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	list := func() []cloudstorage.Checkpoint {
		t.Helper()
		items, err := store.(cloudstorage.CheckpointLister).List(ctx)
		if err != nil {
			t.Fatal(err)
		}
		return items
	}
	run("upload-interrupt", 73)
	items := list()
	if len(items) != 1 {
		t.Fatalf("expected an unfinished upload checkpoint, got %d", len(items))
	}
	state, err := items[0].DecodeUpload()
	if err != nil || len(state.Completed) == 0 || int64(len(state.Completed)) >= state.TotalParts {
		t.Fatalf("missing unfinished multipart progress: %v", err)
	}
	t.Logf("confirmed %d/%d upload parts before process exit", len(state.Completed), state.TotalParts)
	run("upload-resume", 0)
	data, err := os.ReadFile(filepath.Join(dir, "result.json"))
	if err != nil {
		t.Fatal(err)
	}
	var uploaded cloudstorage.ObjectInfo
	if json.Unmarshal(data, &uploaded) != nil || uploaded.ID != state.Session["fid"] {
		t.Fatal("resumed upload did not reuse the original FID")
	}
	if len(list()) != 0 {
		t.Fatal("upload checkpoint was not cleared")
	}
	run("download-interrupt", 74)
	items = list()
	if len(items) != 1 {
		t.Fatalf("expected a download checkpoint, got %d", len(items))
	}
	partial, err := items[0].DecodeDownload()
	if err != nil || partial.Offset <= 0 || partial.Offset >= partial.Size {
		t.Fatalf("missing partial download progress: %v", err)
	}
	t.Logf("confirmed %d/%d download bytes before process exit", partial.Offset, partial.Size)
	run("download-resume", 0)
	if len(list()) != 0 {
		t.Fatal("download checkpoint was not cleared")
	}
	hash := func(path string) (string, int64) {
		t.Helper()
		f, err := os.Open(path)
		if err != nil {
			t.Fatal(err)
		}
		value, size, hashErr := liveFileSHA256(f)
		if err := errors.Join(hashErr, f.Close()); err != nil {
			t.Fatal(err)
		}
		return value, size
	}
	want, size := hash(sourcePath)
	got, downloaded := hash(filepath.Join(dir, "download.bin"))
	if want != got || size != downloaded || size != uploaded.Size {
		t.Fatal("resumed download does not match the complete source SHA-256")
	}
	t.Logf("cross-process upload/download recovery verified: size=%d sha256=%s", size, want)
	client, err := quark.Connect(ctx, quark.Config{StateDir: dir}, cloudstorage.Config{Prefix: prefix})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if os.Getenv("QUARK_TEST_KEEP_REMOTE") != "1" {
		if err := client.Delete(ctx, "media/interrupted.bin"); err != nil {
			t.Fatal(err)
		}
		if _, err := client.Stat(ctx, "media/interrupted.bin"); !errors.Is(err, cloudstorage.ErrNotFound) {
			t.Fatalf("test object still visible after deletion: %v", err)
		}
	}
	// Preserve all diagnostic files on failure; remove only this run's fixtures
	// and abandoned upload sources after successful content verification.
	for _, path := range []string{sourcePath, filepath.Join(dir, "download.bin"), filepath.Join(dir, "result.json")} {
		if err := os.Remove(path); err != nil {
			t.Fatal(err)
		}
	}
	old := time.Now().Add(-48 * time.Hour)
	if err := os.Chtimes(state.Source, old, old); err != nil {
		t.Fatal(err)
	}
	if _, err := cloudstorage.CleanupTempUploads(ctx, dir, cloudstorage.TempCleanupOptions{MinAge: 24 * time.Hour, Checkpoints: store}); err != nil {
		t.Fatal(err)
	}
}
