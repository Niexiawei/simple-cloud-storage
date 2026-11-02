package cloudstorage

import (
	"context"
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func testClient(t *testing.T, provider *mockProvider) *Client {
	t.Helper()
	client, err := NewClient(provider, Config{
		Prefix:             "apps/test",
		PartSize:           4,
		MultipartThreshold: 4,
		TempDir:            t.TempDir(),
	})
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func sizePointer(size int64) *int64 { return &size }

func TestWriterKnownSizeMultipart(t *testing.T) {
	provider := newMockProvider()
	client := testClient(t, provider)
	var progress []Progress
	writer, err := client.NewWriter(context.Background(), "media/a.bin", WriterOptions{
		Size:     sizePointer(10),
		Progress: func(p Progress) { progress = append(progress, p) },
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Write([]byte("abcde")); err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Write([]byte("fghij")); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	if got := string(provider.objects["apps/test/media/a.bin"]); got != "abcdefghij" {
		t.Fatalf("uploaded content = %q", got)
	}
	if len(provider.sessions) != 1 {
		t.Fatalf("multipart sessions = %d", len(provider.sessions))
	}
	session := provider.sessions[0]
	if !session.complete || session.aborted {
		t.Fatalf("unexpected session state: complete=%v aborted=%v", session.complete, session.aborted)
	}
	if want := []int64{4, 4, 2}; !reflect.DeepEqual(session.sizes, want) {
		t.Fatalf("part sizes = %v, want %v", session.sizes, want)
	}
	if got := writer.Info(); got.Key != "media/a.bin" || got.Size != 10 {
		t.Fatalf("writer info = %+v", got)
	}
	if len(progress) != 3 || progress[len(progress)-1].Transferred != 10 {
		t.Fatalf("progress = %+v", progress)
	}
	if err := writer.Abort(context.Background()); err != nil {
		t.Fatal(err)
	}
	if session.aborted {
		t.Fatal("successful session was aborted")
	}
}

func TestWriterUnknownSizeSpoolsAndHashes(t *testing.T) {
	provider := newMockProvider()
	client := testClient(t, provider)
	tempDir := t.TempDir()
	writer, err := client.NewWriter(context.Background(), "unknown.bin", WriterOptions{TempDir: tempDir})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Write([]byte("abcdefghij")); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	if len(provider.sessions) != 1 {
		t.Fatalf("multipart sessions = %d", len(provider.sessions))
	}
	opts := provider.sessions[0].opts
	assertHashes(t, opts, []byte("abcdefghij"))
	entries, err := os.ReadDir(tempDir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("temporary files remain: %v", entries)
	}
}

func TestWriterSmallUpload(t *testing.T) {
	provider := newMockProvider()
	client := testClient(t, provider)
	info, err := client.Upload(context.Background(), "small.txt", stringsReader("abc"), UploadOptions{Size: sizePointer(3)})
	if err != nil {
		t.Fatal(err)
	}
	if info.Size != 3 || info.Key != "small.txt" {
		t.Fatalf("info = %+v", info)
	}
	if len(provider.uploads) != 1 || len(provider.sessions) != 0 {
		t.Fatalf("simple=%d multipart=%d", len(provider.uploads), len(provider.sessions))
	}
	assertHashes(t, provider.uploads[0].opts, []byte("abc"))
}

func TestWriterProviderConstraintUsesSpool(t *testing.T) {
	provider := newMockProvider()
	provider.caps = Capabilities{SupportsStreamingMultipart: false, RequiresFullHash: true}
	client := testClient(t, provider)
	writer, err := client.NewWriter(context.Background(), "quark.bin", WriterOptions{Size: sizePointer(10)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Write([]byte("abcdefghij")); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	if len(provider.sessions) != 1 {
		t.Fatalf("multipart sessions = %d", len(provider.sessions))
	}
	assertHashes(t, provider.sessions[0].opts, []byte("abcdefghij"))
}

func TestWriterAbort(t *testing.T) {
	provider := newMockProvider()
	client := testClient(t, provider)
	writer, err := client.NewWriter(context.Background(), "abort.bin", WriterOptions{Size: sizePointer(10)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Write([]byte("abcd")); err != nil {
		t.Fatal(err)
	}
	if err := writer.Abort(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := writer.Abort(context.Background()); err != nil {
		t.Fatal(err)
	}
	if provider.sessions[0].aborts != 1 {
		t.Fatalf("abort calls = %d", provider.sessions[0].aborts)
	}
	if _, err := writer.Write([]byte("x")); !errors.Is(err, ErrWriterAborted) {
		t.Fatalf("Write after Abort error = %v", err)
	}
	if err := writer.Close(); !errors.Is(err, ErrWriterAborted) {
		t.Fatalf("Close after Abort error = %v", err)
	}
}

func TestWriterAbortCanRetryCleanup(t *testing.T) {
	provider := newMockProvider()
	client := testClient(t, provider)
	writer, err := client.NewWriter(context.Background(), "retry-abort.bin", WriterOptions{Size: sizePointer(10)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Write([]byte("abcd")); err != nil {
		t.Fatal(err)
	}
	session := provider.sessions[0]
	session.abortFailures = 1
	if err := writer.Abort(context.Background()); !errors.Is(err, ErrTemporary) {
		t.Fatalf("first Abort error = %v", err)
	}
	if err := writer.Abort(context.Background()); err != nil {
		t.Fatalf("second Abort error = %v", err)
	}
	if session.aborts != 2 || !session.aborted {
		t.Fatalf("session aborts=%d aborted=%v", session.aborts, session.aborted)
	}
}

func TestWriterCancelBeforeClose(t *testing.T) {
	provider := newMockProvider()
	client := testClient(t, provider)
	ctx, cancel := context.WithCancel(context.Background())
	writer, err := client.NewWriter(ctx, "cancelled.bin", WriterOptions{Size: sizePointer(3)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Write([]byte("abc")); err != nil {
		t.Fatal(err)
	}
	cancel()
	if err := writer.Close(); !errors.Is(err, context.Canceled) {
		t.Fatalf("Close error = %v, want context.Canceled", err)
	}
	if len(provider.uploads) != 0 || len(provider.objects) != 0 {
		t.Fatalf("canceled upload committed: uploads=%d objects=%d", len(provider.uploads), len(provider.objects))
	}
}

func TestWriterAbortRemovesSpool(t *testing.T) {
	provider := newMockProvider()
	client := testClient(t, provider)
	tempDir := t.TempDir()
	writer, err := client.NewWriter(context.Background(), "abort.bin", WriterOptions{TempDir: tempDir})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Write([]byte("data")); err != nil {
		t.Fatal(err)
	}
	if err := writer.Abort(context.Background()); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(tempDir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("temporary files remain: %v", entries)
	}
}

func TestWriterSizeMismatch(t *testing.T) {
	provider := newMockProvider()
	client := testClient(t, provider)
	writer, err := client.NewWriter(context.Background(), "short.bin", WriterOptions{Size: sizePointer(10)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Write([]byte("abc")); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); !errors.Is(err, ErrSizeMismatch) {
		t.Fatalf("Close error = %v", err)
	}
	if !provider.sessions[0].aborted {
		t.Fatal("size mismatch did not abort multipart session")
	}
}

func TestMultipartFailureAborts(t *testing.T) {
	provider := newMockProvider()
	provider.failPart = 2
	client := testClient(t, provider)
	_, err := client.Upload(context.Background(), "failure.bin", stringsReader("abcdefghij"), UploadOptions{})
	if !errors.Is(err, ErrTemporary) {
		t.Fatalf("Upload error = %v", err)
	}
	if len(provider.sessions) != 1 || !provider.sessions[0].aborted {
		t.Fatal("failed multipart session was not aborted")
	}
}

func assertHashes(t *testing.T, opts UploadOptions, data []byte) {
	t.Helper()
	md5Sum := md5.Sum(data)
	sha1Sum := sha1.Sum(data)
	sha256Sum := sha256.Sum256(data)
	if opts.MD5 != hex.EncodeToString(md5Sum[:]) || opts.SHA1 != hex.EncodeToString(sha1Sum[:]) || opts.SHA256 != hex.EncodeToString(sha256Sum[:]) {
		t.Fatalf("hashes do not match content: md5=%q sha1=%q sha256=%q", opts.MD5, opts.SHA1, opts.SHA256)
	}
	if opts.Size == nil || *opts.Size != int64(len(data)) {
		t.Fatalf("size option = %v, want %d", opts.Size, len(data))
	}
}

type stringReader struct {
	data string
	off  int
}

func stringsReader(data string) io.Reader { return &stringReader{data: data} }

func (r *stringReader) Read(p []byte) (int, error) {
	if r.off == len(r.data) {
		return 0, io.EOF
	}
	n := copy(p, r.data[r.off:])
	r.off += n
	return n, nil
}

func TestUploadFile(t *testing.T) {
	provider := newMockProvider()
	client := testClient(t, provider)
	src := filepath.Join(t.TempDir(), "source.bin")
	if err := os.WriteFile(src, []byte("file-content"), 0o600); err != nil {
		t.Fatal(err)
	}
	info, err := client.UploadFile(context.Background(), "file.bin", src, UploadOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if info.Size != 12 || string(provider.objects["apps/test/file.bin"]) != "file-content" {
		t.Fatalf("info=%+v content=%q", info, provider.objects["apps/test/file.bin"])
	}
}
