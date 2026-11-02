package cloudstorage

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sort"
)

type recordedUpload struct {
	path string
	opts UploadOptions
	data []byte
}

type mockProvider struct {
	caps        Capabilities
	objects     map[string][]byte
	uploads     []recordedUpload
	sessions    []*mockMultipart
	failPart    int
	completeErr error
	abortHook   func(context.Context) error
}

func newMockProvider() *mockProvider {
	return &mockProvider{
		caps:    Capabilities{SupportsStreamingMultipart: true},
		objects: make(map[string][]byte),
	}
}

func (p *mockProvider) Name() string { return "mock" }

func (p *mockProvider) AccountID(context.Context) (string, error) { return "account", nil }

func (p *mockProvider) Capabilities() Capabilities { return p.caps }

func (p *mockProvider) Stat(_ context.Context, path string) (ObjectInfo, error) {
	data, ok := p.objects[path]
	if !ok {
		return ObjectInfo{}, ErrNotFound
	}
	return ObjectInfo{ID: "id-" + path, Size: int64(len(data))}, nil
}

func (p *mockProvider) Open(_ context.Context, path string, opts ReadOptions) (io.ReadCloser, error) {
	data, ok := p.objects[path]
	if !ok {
		return nil, ErrNotFound
	}
	if opts.Offset > int64(len(data)) {
		return io.NopCloser(bytes.NewReader(nil)), nil
	}
	data = data[opts.Offset:]
	if opts.Length > 0 && opts.Length < int64(len(data)) {
		data = data[:opts.Length]
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

func (p *mockProvider) Upload(_ context.Context, path string, r io.Reader, opts UploadOptions) (ObjectInfo, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return ObjectInfo{}, err
	}
	p.objects[path] = append([]byte(nil), data...)
	p.uploads = append(p.uploads, recordedUpload{path: path, opts: opts, data: append([]byte(nil), data...)})
	return ObjectInfo{ID: "id-" + path, Size: int64(len(data)), MD5: opts.MD5, SHA1: opts.SHA1, SHA256: opts.SHA256}, nil
}

func (p *mockProvider) CreateMultipart(_ context.Context, path string, opts UploadOptions) (MultipartSession, error) {
	s := &mockMultipart{provider: p, path: path, opts: opts, parts: make(map[int][]byte)}
	p.sessions = append(p.sessions, s)
	return s, nil
}

func (p *mockProvider) Delete(_ context.Context, path string) error {
	if _, ok := p.objects[path]; !ok {
		return ErrNotFound
	}
	delete(p.objects, path)
	return nil
}

type mockMultipart struct {
	provider      *mockProvider
	path          string
	opts          UploadOptions
	parts         map[int][]byte
	sizes         []int64
	complete      bool
	aborted       bool
	aborts        int
	abortFailures int
}

func (s *mockMultipart) UploadPart(_ context.Context, number int, r io.Reader, size int64) (PartInfo, error) {
	if s.aborted {
		return PartInfo{}, fmt.Errorf("session aborted")
	}
	if s.provider.failPart == number {
		return PartInfo{}, ErrTemporary
	}
	data, err := io.ReadAll(r)
	if err != nil {
		return PartInfo{}, err
	}
	if int64(len(data)) != size {
		return PartInfo{}, ErrSizeMismatch
	}
	s.parts[number] = append([]byte(nil), data...)
	s.sizes = append(s.sizes, size)
	return PartInfo{ETag: fmt.Sprintf("etag-%d", number)}, nil
}

func (s *mockMultipart) Complete(_ context.Context, _ []PartInfo) (ObjectInfo, error) {
	if s.provider.completeErr != nil {
		return ObjectInfo{}, s.provider.completeErr
	}
	if s.aborted {
		return ObjectInfo{}, fmt.Errorf("session aborted")
	}
	numbers := make([]int, 0, len(s.parts))
	for number := range s.parts {
		numbers = append(numbers, number)
	}
	sort.Ints(numbers)
	var data []byte
	for _, number := range numbers {
		data = append(data, s.parts[number]...)
	}
	s.provider.objects[s.path] = data
	s.complete = true
	return ObjectInfo{ID: "id-" + s.path, Size: int64(len(data)), MD5: s.opts.MD5, SHA1: s.opts.SHA1, SHA256: s.opts.SHA256}, nil
}

func (s *mockMultipart) Abort(ctx context.Context) error {
	s.aborts++
	if s.provider.abortHook != nil {
		if err := s.provider.abortHook(ctx); err != nil {
			return err
		}
	}
	if s.abortFailures > 0 {
		s.abortFailures--
		return ErrTemporary
	}
	s.aborted = true
	return nil
}
