package cloudstorage

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"
)

type pathRecordingProvider struct {
	*mockProvider
	paths []string
}

func (p *pathRecordingProvider) Stat(ctx context.Context, path string) (ObjectInfo, error) {
	p.paths = append(p.paths, path)
	return p.mockProvider.Stat(ctx, path)
}
func (p *pathRecordingProvider) Open(ctx context.Context, path string, opts ReadOptions) (io.ReadCloser, error) {
	p.paths = append(p.paths, path)
	return p.mockProvider.Open(ctx, path, opts)
}
func (p *pathRecordingProvider) Upload(ctx context.Context, path string, r io.Reader, opts UploadOptions) (ObjectInfo, error) {
	p.paths = append(p.paths, path)
	return p.mockProvider.Upload(ctx, path, r, opts)
}
func (p *pathRecordingProvider) CreateMultipart(ctx context.Context, path string, opts UploadOptions) (MultipartSession, error) {
	p.paths = append(p.paths, path)
	return p.mockProvider.CreateMultipart(ctx, path, opts)
}
func (p *pathRecordingProvider) Delete(ctx context.Context, path string) error {
	p.paths = append(p.paths, path)
	return p.mockProvider.Delete(ctx, path)
}

func TestAllOperationsShareCanonicalPrefix(t *testing.T) {
	for _, prefix := range []string{"/ app /", " /apps/demo/ ", "app/ inner /dir"} {
		t.Run(prefix, func(t *testing.T) {
			p := &pathRecordingProvider{mockProvider: newMockProvider()}
			c, err := NewClient(p, Config{Prefix: prefix, PartSize: 4, MultipartThreshold: 4, TempDir: t.TempDir()})
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			key := "docs/a"
			if _, err := c.Upload(ctx, key, stringsReader("abc"), UploadOptions{Size: sizePointer(3)}); err != nil {
				t.Fatal(err)
			}
			dir := t.TempDir()
			src := filepath.Join(dir, "src")
			if err := os.WriteFile(src, []byte("abcdefgh"), 0600); err != nil {
				t.Fatal(err)
			}
			if _, err := c.UploadFile(ctx, key, src, UploadOptions{}); err != nil {
				t.Fatal(err)
			}
			w, err := c.NewWriter(ctx, key, WriterOptions{Size: sizePointer(1)})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := w.Write([]byte("x")); err != nil {
				t.Fatal(err)
			}
			if err := w.Close(); err != nil {
				t.Fatal(err)
			}
			if _, err := c.Stat(ctx, key); err != nil {
				t.Fatal(err)
			}
			r, err := c.Open(ctx, key, ReadOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if err := r.Close(); err != nil {
				t.Fatal(err)
			}
			if err := c.DownloadToFile(ctx, key, filepath.Join(dir, "dst"), DownloadOptions{}); err != nil {
				t.Fatal(err)
			}
			if err := c.Delete(ctx, key); err != nil {
				t.Fatal(err)
			}
			want := c.Prefix() + "/" + key
			if len(p.paths) != 7 {
				t.Fatalf("missing operations: %v", p.paths)
			}
			for _, path := range p.paths {
				if path != want {
					t.Fatalf("path=%q want=%q", path, want)
				}
			}
		})
	}
}
