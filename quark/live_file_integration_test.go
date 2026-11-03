//go:build integration

package quark_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	cloudstorage "github.com/Niexiawei/simple-cloud-storage"
	"github.com/Niexiawei/simple-cloud-storage/quark"
)

/*
$env:QUARK_COOKIES = '<完整 Cookie>'
$env:QUARK_LIVE_FILE_TEST = '1'
go test -tags=integration ./quark -run '^TestLiveQuarkInputMP4$' -count=1 -v -timeout=2h
*/

// TestLiveQuarkInputMP4 uploads the real fixture, reconnects using persisted
// credentials, checks byte ranges and verifies a complete downloaded copy.
// It deletes only its own object on success, unless QUARK_TEST_KEEP_REMOTE=1.
// Failed runs retain the remote object/checkpoint for investigation.
func TestLiveQuarkInputMP4(t *testing.T) {
	if os.Getenv("QUARK_LIVE_FILE_TEST") != "1" {
		t.Skip("set QUARK_LIVE_FILE_TEST=1 and configure QUARK_COOKIES or a persisted CookieFile")
	}
	// go test starts in this package directory. Keep default credential/state
	// paths at the module root, matching the documented command below.
	t.Chdir("..")
	const sourcePath = `D:\input.mp4`
	source, err := os.Open(sourcePath)
	if err != nil {
		t.Fatalf("open fixture: %v", err)
	}
	t.Cleanup(func() {
		if err := source.Close(); err != nil {
			t.Errorf("close fixture: %v", err)
		}
	})
	stat, err := source.Stat()
	if err != nil {
		t.Fatal(err)
	}
	if !stat.Mode().IsRegular() || stat.Size() == 0 {
		t.Fatal("D:\\input.mp4 must be a non-empty regular file")
	}
	expectedHash, size, err := liveFileSHA256(source)
	if err != nil {
		t.Fatalf("hash fixture: %v", err)
	}
	if size != stat.Size() {
		t.Fatal("fixture size changed while hashing")
	}

	var nonce [8]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		t.Fatal(err)
	}
	base := strings.Trim(strings.TrimSpace(os.Getenv("QUARK_TEST_PREFIX")), "/")
	if base == "" {
		base = "integration/simple-cloud-storage"
	}
	runID := os.Getenv("QUARK_TEST_RUN_ID")
	if runID == "" {
		runID = time.Now().UTC().Format("20060102T150405") + "-" + hex.EncodeToString(nonce[:])
	}
	if runID == "." || runID == ".." || strings.ContainsAny(runID, "/\\") {
		t.Fatal("QUARK_TEST_RUN_ID must be a single directory name")
	}
	prefix := base + "/" + runID
	const key = "media/input.mp4"
	t.Logf("source=%s size=%d bytes sha256=%s", sourcePath, size, expectedHash)
	t.Logf("remote=%s/%s", prefix, key)

	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Minute)
	defer cancel()
	cfg := cloudstorage.Config{Prefix: prefix, TempDir: t.TempDir()}
	connect := func() *quark.Connection {
		t.Helper()
		client, err := quark.Connect(ctx, quark.Config{}, cfg)
		if err != nil {
			t.Fatalf("connect quark: %v", err)
		}
		return client
	}
	client := connect()
	t.Cleanup(func() {
		if client != nil {
			if err := client.Close(); err != nil {
				t.Errorf("close quark: %v", err)
			}
		}
	})

	uploaded, err := client.UploadFile(ctx, key, sourcePath, cloudstorage.UploadOptions{
		Resume: true, Progress: liveFileProgress(t, "upload"),
	})
	if err != nil {
		t.Fatalf("upload (remote path logged above; state retained): %v", err)
	}
	if uploaded.ID == "" || uploaded.Key != key || uploaded.Size != size {
		t.Fatalf("unexpected upload result: %+v", uploaded)
	}
	if err := client.Provider.RefreshCookies(ctx); err != nil {
		t.Fatalf("refresh/persist cookies: %v", err)
	}
	cookiePath := os.Getenv("QUARK_COOKIE_FILE")
	if cookiePath == "" {
		cookiePath = quark.DefaultCookieFile
	}
	if info, err := os.Stat(cookiePath); err != nil || info.Size() == 0 {
		t.Fatalf("CookieFile was not persisted: %v", err)
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	client = nil
	// Reconnection must now work from CookieFile alone. Values are never logged;
	// testing restores the process environment when this test exits.
	t.Setenv("QUARK_COOKIES", "")
	t.Setenv("QUARK_COOKIE", "")
	client = connect()
	remote, err := client.Stat(ctx, key)
	if err != nil {
		t.Fatalf("stat after reconnect: %v", err)
	}
	if remote.ID != uploaded.ID || remote.Size != size || remote.Key != key {
		t.Fatalf("object identity changed: %+v", remote)
	}

	length := min(int64(64<<10), size)
	for _, sample := range []struct {
		name   string
		offset int64
	}{
		{"start", 0}, {"middle", (size - length) / 2}, {"tail", size - length},
	} {
		if !t.Run("range/"+sample.name, func(t *testing.T) {
			expected := make([]byte, int(length))
			if _, err := source.ReadAt(expected, sample.offset); err != nil {
				t.Fatal(err)
			}
			r, err := client.Open(ctx, key, cloudstorage.ReadOptions{Offset: sample.offset, Length: length})
			if err != nil {
				t.Fatal(err)
			}
			actual, readErr := io.ReadAll(io.LimitReader(r, length+1))
			closeErr := r.Close()
			if err := errors.Join(readErr, closeErr); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(actual, expected) {
				t.Fatalf("range bytes differ at offset %d", sample.offset)
			}
		}) {
			t.Fatal("Range validation failed; preserving remote object")
		}
	}

	destination := filepath.Join(t.TempDir(), "input.download.mp4")
	if err := client.DownloadToFile(ctx, key, destination, cloudstorage.DownloadOptions{
		Resume: true, Progress: liveFileProgress(t, "download"),
	}); err != nil {
		t.Fatalf("download: %v", err)
	}
	download, err := os.Open(destination)
	if err != nil {
		t.Fatal(err)
	}
	actualHash, actualSize, hashErr := liveFileSHA256(download)
	if err := errors.Join(hashErr, download.Close()); err != nil {
		t.Fatal(err)
	}
	if actualSize != size || actualHash != expectedHash {
		t.Fatalf("download differs: size=%d sha256=%s", actualSize, actualHash)
	}
	if _, err := os.Stat(destination + ".part"); !os.IsNotExist(err) {
		t.Fatalf("completed download left .part: %v", err)
	}
	currentSource, err := source.Stat()
	if err != nil {
		t.Fatal(err)
	}
	if currentSource.Size() != stat.Size() || !currentSource.ModTime().Equal(stat.ModTime()) {
		t.Fatal("source fixture changed during the test")
	}
	t.Log("upload, persisted-cookie reconnect, Range and full SHA-256 validation passed")
	if os.Getenv("QUARK_TEST_KEEP_REMOTE") == "1" {
		t.Logf("retained remote object: %s/%s", prefix, key)
		return
	}
	if err := client.Delete(ctx, key); err != nil {
		t.Fatalf("delete test object: %v", err)
	}
	if _, err := client.Stat(ctx, key); !errors.Is(err, cloudstorage.ErrNotFound) {
		t.Fatalf("Stat after Delete: %v", err)
	}
	t.Log("test object deleted; empty test directories may remain")
}

func liveFileSHA256(r io.Reader) (string, int64, error) {
	h := sha256.New()
	size, err := io.Copy(h, r)
	return hex.EncodeToString(h.Sum(nil)), size, err
}

func liveFileProgress(t *testing.T, stage string) cloudstorage.ProgressFunc {
	last := -10
	return func(progress cloudstorage.Progress) {
		if progress.Total <= 0 {
			return
		}
		percent := int(100 * progress.Transferred / progress.Total)
		if percent >= last+10 || (percent == 100 && last != 100) {
			last = percent
			t.Logf("%s %d%% (%d/%d bytes)", stage, percent, progress.Transferred, progress.Total)
		}
	}
}
