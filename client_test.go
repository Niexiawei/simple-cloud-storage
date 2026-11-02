package cloudstorage

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func TestClientPrefixOperationsAndDownload(t *testing.T) {
	provider := newMockProvider()
	client := testClient(t, provider)
	ctx := context.Background()
	if _, err := client.Upload(ctx, "docs/a.txt", stringsReader("0123456789"), UploadOptions{}); err != nil {
		t.Fatal(err)
	}
	info, err := client.Stat(ctx, "docs/a.txt")
	if err != nil {
		t.Fatal(err)
	}
	if info.Key != "docs/a.txt" || info.Size != 10 {
		t.Fatalf("Stat = %+v", info)
	}
	r, err := client.Open(ctx, "docs/a.txt", ReadOptions{Offset: 3, Length: 4})
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	if string(data) != "3456" {
		t.Fatalf("range content = %q", data)
	}
	r, err = client.Open(ctx, "docs/a.txt", ReadOptions{Offset: 10})
	if err != nil {
		t.Fatal(err)
	}
	data, err = io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	_ = r.Close()
	if len(data) != 0 {
		t.Fatalf("range at EOF = %q, want empty", data)
	}

	dst := filepath.Join(t.TempDir(), "download.txt")
	if err := client.DownloadToFile(ctx, "docs/a.txt", dst, DownloadOptions{}); err != nil {
		t.Fatal(err)
	}
	data, err = os.ReadFile(dst)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "0123456789" {
		t.Fatalf("download content = %q", data)
	}
	if _, err := os.Stat(dst + ".part"); !os.IsNotExist(err) {
		t.Fatalf("part file remains: %v", err)
	}
	if err := client.DownloadToFile(ctx, "docs/a.txt", dst, DownloadOptions{}); err != nil {
		t.Fatalf("overwrite download error = %v", err)
	}
	data, err = os.ReadFile(dst)
	if err != nil || string(data) != "0123456789" {
		t.Fatalf("overwritten download content = %q, err=%v", data, err)
	}
	if err := client.Delete(ctx, "docs/a.txt"); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Stat(ctx, "docs/a.txt"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Stat after Delete error = %v", err)
	}
}

func TestClientValidation(t *testing.T) {
	provider := newMockProvider()
	if _, err := NewClient(provider, Config{Prefix: ""}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("empty Prefix error = %v", err)
	}
	if _, err := NewClient(provider, Config{Prefix: "app", Concurrency: 2}); !errors.Is(err, ErrUnsupported) {
		t.Fatalf("Concurrency error = %v", err)
	}
	client := testClient(t, provider)
	if _, err := client.Upload(context.Background(), "../escape", stringsReader("x"), UploadOptions{}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("path traversal error = %v", err)
	}
	if _, err := client.Open(context.Background(), "a", ReadOptions{Offset: -1}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("negative range error = %v", err)
	}
}

func TestDownloadRemovesPartOnFailure(t *testing.T) {
	provider := newMockProvider()
	client := testClient(t, provider)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	dst := filepath.Join(t.TempDir(), "download.txt")
	provider.objects["apps/test/a"] = []byte("data")
	err := client.DownloadToFile(ctx, "a", dst, DownloadOptions{})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("DownloadToFile error = %v", err)
	}
	if _, statErr := os.Stat(dst + ".part"); !os.IsNotExist(statErr) {
		t.Fatalf("part file remains: %v", statErr)
	}
}
