//go:build integration

package quark

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	cloudstorage "github.com/Niexiawei/simple-cloud-storage"
)

func TestUploadProcessHelper(t *testing.T) {
	mode := os.Getenv("SCS_UPLOAD_PROCESS_MODE")
	if mode == "" {
		t.Skip("subprocess helper")
	}
	dir := os.Getenv("SCS_UPLOAD_PROCESS_DIR")
	c, err := Connect(context.Background(), Config{
		Cookies: "__pus=bootstrap; __puus=old", CookieFile: filepath.Join(dir, "cookies.json"),
		StateDir: filepath.Join(dir, "state"), APIBaseURL: os.Getenv("SCS_UPLOAD_PROCESS_API"), DisableAutoRefresh: true,
	}, cloudstorage.Config{Prefix: "apps/process", TempDir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	first := int64(-1)
	_, err = c.Upload(context.Background(), "a", strings.NewReader("abcdefghij"), cloudstorage.UploadOptions{
		Resume: true, ResumeID: "process", Progress: func(p cloudstorage.Progress) {
			if first == -1 {
				first = p.Transferred
			}
			if mode == "interrupt" && p.Transferred > 0 && p.Transferred < p.Total {
				os.Exit(73) // Simulate process loss: no deferred cleanup or connection Close.
			}
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if mode == "interrupt" || first != 4 {
		t.Fatalf("resume did not begin from one confirmed part: %d", first)
	}
}

func TestUploadSurvivesProcessExit(t *testing.T) {
	f := newFake(t)
	server := httptest.NewServer(http.HandlerFunc(f.serve))
	defer server.Close()
	f.server = server
	dir := t.TempDir()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	run := func(mode string) error {
		t.Helper()
		ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
		defer cancel()
		cmd := exec.CommandContext(ctx, executable, "-test.run=^TestUploadProcessHelper$", "-test.timeout=25s")
		cmd.Env = append(os.Environ(), "SCS_UPLOAD_PROCESS_MODE="+mode, "SCS_UPLOAD_PROCESS_DIR="+dir, "SCS_UPLOAD_PROCESS_API="+server.URL+"/api")
		output, err := cmd.CombinedOutput()
		if err != nil {
			t.Logf("subprocess: %s", output)
		}
		return err
	}
	var exited *exec.ExitError
	if err := run("interrupt"); !errors.As(err, &exited) || exited.ExitCode() != 73 {
		t.Fatalf("expected deliberate process exit, got %v", err)
	}
	store, err := cloudstorage.NewFileCheckpointStore(filepath.Join(dir, "state"))
	if err != nil {
		t.Fatal(err)
	}
	checkpoints, err := store.(cloudstorage.CheckpointLister).List(context.Background())
	if err != nil || len(checkpoints) != 1 {
		t.Fatalf("checkpoint did not survive process loss: %d, %v", len(checkpoints), err)
	}
	state, err := checkpoints[0].DecodeUpload()
	if err != nil || len(state.Completed) != 1 || state.Completed[0].Size != 4 {
		t.Fatalf("missing confirmed part: %+v, %v", state.Completed, err)
	}
	if data, err := os.ReadFile(state.Source); err != nil || string(data) != "abcdefghij" {
		t.Fatal("complete spool did not survive process loss")
	}
	if err := run("resume"); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	pre, first, finished, data := f.preCalls, f.partCalls[1], f.finishCalls, string(f.data["file-task-1"])
	f.mu.Unlock()
	if pre != 1 || first != 1 || finished != 1 || data != "abcdefghij" {
		t.Fatalf("session was not reused: pre=%d first=%d finish=%d data=%q", pre, first, finished, data)
	}
	checkpoints, err = store.(cloudstorage.CheckpointLister).List(context.Background())
	if err != nil || len(checkpoints) != 0 {
		t.Fatalf("completed upload left checkpoints: %d, %v", len(checkpoints), err)
	}
	old := time.Now().Add(-48 * time.Hour)
	if err := os.Chtimes(state.Source, old, old); err != nil {
		t.Fatal(err)
	}
	removed, err := cloudstorage.CleanupTempUploads(context.Background(), dir, cloudstorage.TempCleanupOptions{MinAge: 24 * time.Hour, Checkpoints: store})
	if err != nil || len(removed) != 1 || !removed[0].Removed {
		t.Fatalf("abandoned process spool was not cleaned: %+v, %v", removed, err)
	}
}
