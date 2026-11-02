package cloudstorage

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
)

type interruptedDownload struct{ *mockProvider }

func (p interruptedDownload) Open(context.Context, string, ReadOptions) (io.ReadCloser, error) {
	return io.NopCloser(&partialDownloadReader{}), nil
}

type partialDownloadReader struct{ read bool }

func (r *partialDownloadReader) Read(p []byte) (int, error) {
	if r.read {
		return 0, ErrTemporary
	}
	r.read = true
	return copy(p, "part"), nil
}

func TestDownloadPartialPolicy(t *testing.T) {
	for _, keep := range []bool{false, true} {
		name := "remove"
		if keep {
			name = "retain"
		}
		t.Run(name, func(t *testing.T) {
			c, err := NewClient(interruptedDownload{newMockProvider()}, Config{Prefix: "app"})
			if err != nil {
				t.Fatal(err)
			}
			dst := filepath.Join(t.TempDir(), "file")
			if err := os.WriteFile(dst, []byte("old"), 0600); err != nil {
				t.Fatal(err)
			}
			var progress []Progress
			err = c.DownloadToFile(context.Background(), "a", dst, DownloadOptions{KeepPartial: keep, Progress: func(p Progress) { progress = append(progress, p) }})
			if !errors.Is(err, ErrTemporary) {
				t.Fatal(err)
			}
			if data, err := os.ReadFile(dst); err != nil || string(data) != "old" {
				t.Fatal("target overwritten on failure")
			}
			data, err := os.ReadFile(dst + ".part")
			if keep {
				if err != nil || string(data) != "part" {
					t.Fatalf("partial = %q %v", data, err)
				}
			} else if !os.IsNotExist(err) {
				t.Fatalf("partial remains: %q %v", data, err)
			}
			if len(progress) != 1 || progress[0].Transferred != 4 || progress[0].Total != -1 {
				t.Fatalf("progress=%+v", progress)
			}
		})
	}
}
