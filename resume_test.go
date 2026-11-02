package cloudstorage

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestCheckpointIdentityIsolation(t *testing.T) {
	p := newMockProvider()
	c := testClient(t, p)
	ctx := context.Background()
	base, err := c.newCheckpoint(ctx, "a", CheckpointUpload, "writer", json.RawMessage(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	again, err := c.newCheckpoint(ctx, "a", CheckpointUpload, "writer", json.RawMessage(`{"progress":1}`))
	if err != nil || again.ID != base.ID {
		t.Fatalf("unstable identity: %v", err)
	}
	for _, name := range []string{"kind", "provider", "account", "prefix", "key", "resume id"} {
		t.Run(name, func(t *testing.T) {
			candidate := base
			switch name {
			case "kind":
				candidate.Kind = CheckpointDownload
			case "provider":
				candidate.Provider = "other"
			case "account":
				candidate.AccountID = "other"
			case "prefix":
				candidate.Path = "other/a"
			case "key":
				candidate.Path = c.Prefix() + "/b"
			case "resume id":
				candidate.ResumeID = "other"
			}
			if candidate.fingerprint() == base.ID {
				t.Fatal("identity collision")
			}
			if !errors.Is(candidate.Validate(), ErrInvalidCheckpoint) {
				t.Fatal("altered identity accepted")
			}
		})
	}
	// Distinct field boundaries must not collide even when values contain separators.
	a, b := base, base
	a.Provider, a.AccountID = "a/b", "c"
	b.Provider, b.AccountID = "a", "b/c"
	if a.fingerprint() == b.fingerprint() {
		t.Fatal("ambiguous encoding")
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := c.newCheckpoint(cancelled, "a", CheckpointUpload, "", json.RawMessage(`{}`)); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestUploadStateValidationAndRoundTrip(t *testing.T) {
	base := UploadState{Size: 10, SHA256: strings.Repeat("a", 64), PartSize: 4, TotalParts: 3,
		Completed: []PartInfo{{PartNumber: 1, Size: 4, ETag: "one"}, {PartNumber: 3, Size: 2, ETag: "three"}},
		Session:   map[string]string{"upload_id": "id"}}
	cp := testCheckpoint()
	data, err := json.Marshal(base)
	if err != nil {
		t.Fatal(err)
	}
	cp.Data = data
	raw, err := json.Marshal(cp)
	if err != nil {
		t.Fatal(err)
	}
	var loaded Checkpoint
	if err := json.Unmarshal(raw, &loaded); err != nil {
		t.Fatal(err)
	}
	got, err := loaded.DecodeUpload()
	if err != nil || !reflect.DeepEqual(got, base) {
		t.Fatalf("roundtrip = %+v %v", got, err)
	}
	if err := base.MatchSource(10, strings.Repeat("A", 64)); err != nil {
		t.Fatal(err)
	}
	if err := base.MatchSource(11, base.SHA256); !errors.Is(err, ErrInvalidCheckpoint) {
		t.Fatal(err)
	}
	if err := base.MatchSource(10, strings.Repeat("b", 64)); !errors.Is(err, ErrInvalidCheckpoint) {
		t.Fatal(err)
	}
	for _, name := range []string{"negative size", "bad hash", "zero part", "wrong count", "duplicate", "out of range", "wrong size"} {
		t.Run(name, func(t *testing.T) {
			s := base
			s.Completed = append([]PartInfo(nil), base.Completed...)
			switch name {
			case "negative size":
				s.Size = -1
			case "bad hash":
				s.SHA256 = "bad"
			case "zero part":
				s.PartSize = 0
			case "wrong count":
				s.TotalParts++
			case "duplicate":
				s.Completed[1] = s.Completed[0]
			case "out of range":
				s.Completed[0].PartNumber = 4
			case "wrong size":
				s.Completed[1].Size = 4
			}
			if !errors.Is(s.Validate(), ErrInvalidCheckpoint) {
				t.Fatal("invalid progress accepted")
			}
		})
	}
	if err := (UploadState{Size: 0, SHA256: base.SHA256, PartSize: 4}).Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestDownloadStateValidationAndRoundTrip(t *testing.T) {
	dst := filepath.Join(t.TempDir(), "file")
	info := ObjectInfo{ID: "file", Size: 10, ETag: "v1", MD5: strings.Repeat("a", 32), SHA1: strings.Repeat("a", 40), SHA256: strings.Repeat("a", 64), ModTime: time.Unix(100, 0).UTC()}
	base := DownloadState{FileID: info.ID, Size: info.Size, ETag: info.ETag, MD5: info.MD5, SHA1: info.SHA1, SHA256: info.SHA256, ModTime: info.ModTime, Offset: 4, Target: dst, Partial: dst + ".part"}
	cp := testCheckpoint()
	cp.Kind = CheckpointDownload
	cp.ID = cp.fingerprint()
	data, err := json.Marshal(base)
	if err != nil {
		t.Fatal(err)
	}
	cp.Data = data
	loaded, err := cp.DecodeDownload()
	if err != nil || !reflect.DeepEqual(loaded, base) {
		t.Fatalf("roundtrip = %+v %v", loaded, err)
	}
	if err := loaded.MatchObject(info, dst); err != nil {
		t.Fatal(err)
	}
	if err := loaded.MatchObject(info, dst+"-other"); !errors.Is(err, ErrInvalidCheckpoint) {
		t.Fatal(err)
	}
	for _, name := range []string{"id", "size", "etag", "hash", "md5", "sha1", "mod time"} {
		t.Run(name, func(t *testing.T) {
			changed := info
			switch name {
			case "id":
				changed.ID = "other"
			case "size":
				changed.Size++
			case "etag":
				changed.ETag = "v2"
			case "hash":
				changed.SHA256 = strings.Repeat("b", 64)
			case "md5":
				changed.MD5 = strings.Repeat("b", 32)
			case "sha1":
				changed.SHA1 = strings.Repeat("b", 40)
			case "mod time":
				changed.ModTime = info.ModTime.Add(time.Second)
			}
			if !errors.Is(base.MatchObject(changed, dst), ErrInvalidCheckpoint) {
				t.Fatal("changed cloud object accepted")
			}
		})
	}
	for _, name := range []string{"negative", "overflow", "relative", "partial", "no validator"} {
		t.Run(name, func(t *testing.T) {
			s := base
			switch name {
			case "negative":
				s.Offset = -1
			case "overflow":
				s.Offset = s.Size + 1
			case "relative":
				s.Target = "relative"
			case "partial":
				s.Partial = dst + "other.part"
			case "no validator":
				s.ETag, s.SHA256 = "", ""
				s.MD5, s.SHA1, s.ModTime = "", "", time.Time{}
			}
			if !errors.Is(s.Validate(), ErrInvalidCheckpoint) {
				t.Fatal("invalid state accepted")
			}
		})
	}
}

type namespaceProvider struct {
	*mockProvider
	name, account string
	accountErr    error
}

func (p namespaceProvider) Name() string                              { return p.name }
func (p namespaceProvider) AccountID(context.Context) (string, error) { return p.account, p.accountErr }

func TestCheckpointIdentityFromProviderAndLogicalKey(t *testing.T) {
	var first string
	for _, cfg := range []struct{ name, account, prefix, key, resumeID string }{
		{"mock", "account", "/ app /", "docs/a", "writer"},
		{"other", "account", "/ app /", "docs/a", "writer"},
		{"mock", "other", "/ app /", "docs/a", "writer"},
		{"mock", "account", "other", "docs/a", "writer"},
		{"mock", "account", "/ app /", "docs/b", "writer"},
		{"mock", "account", "/ app /", "docs/a", "other"},
	} {
		c, err := NewClient(namespaceProvider{mockProvider: newMockProvider(), name: cfg.name, account: cfg.account}, Config{Prefix: cfg.prefix})
		if err != nil {
			t.Fatal(err)
		}
		cp, err := c.newCheckpoint(context.Background(), cfg.key, CheckpointUpload, cfg.resumeID, json.RawMessage(`{}`))
		if err != nil {
			t.Fatal(err)
		}
		if cp.Path != c.Prefix()+"/"+cfg.key {
			t.Fatalf("identity path mismatch: %+v", cp)
		}
		if first == "" {
			first = cp.ID
		} else if cp.ID == first {
			t.Fatal("provider namespace reused")
		}
	}
	c, err := NewClient(namespaceProvider{mockProvider: newMockProvider(), name: "mock", accountErr: ErrAuthentication}, Config{Prefix: "app"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.newCheckpoint(context.Background(), "a", CheckpointUpload, "", json.RawMessage(`{}`)); !errors.Is(err, ErrAuthentication) {
		t.Fatal(err)
	}
}

func TestResumeRejectedBeforeIO(t *testing.T) {
	for _, configured := range []bool{false, true} {
		name := "missing store"
		if configured {
			name = "unsupported"
		}
		t.Run(name, func(t *testing.T) {
			p := newMockProvider()
			dir := t.TempDir()
			cfg := Config{Prefix: "app", TempDir: dir}
			if configured {
				store, err := NewFileCheckpointStore(t.TempDir())
				if err != nil {
					t.Fatal(err)
				}
				cfg.CheckpointStore = store
			}
			c, err := NewClient(p, cfg)
			if err != nil {
				t.Fatal(err)
			}
			want := ErrInvalidArgument
			if configured {
				want = ErrUnsupported
			}
			if _, err := c.Upload(context.Background(), "a", noRead{t}, UploadOptions{Resume: true}); !errors.Is(err, want) {
				t.Fatal(err)
			}
			if _, err := c.UploadFile(context.Background(), "a", "missing-file", UploadOptions{Resume: true}); !errors.Is(err, want) {
				t.Fatal(err)
			}
			if _, err := c.NewWriter(context.Background(), "a", WriterOptions{Resume: true, ResumeID: "stable"}); !errors.Is(err, want) {
				t.Fatal(err)
			}
			if _, err := c.NewWriter(context.Background(), "a", WriterOptions{Resume: true}); !errors.Is(err, ErrInvalidArgument) {
				t.Fatal(err)
			}
			if _, err := c.NewWriter(context.Background(), "a", WriterOptions{ResumeID: "orphan"}); !errors.Is(err, ErrInvalidArgument) {
				t.Fatal(err)
			}
			dst := filepath.Join(dir, "file")
			for _, path := range []string{dst, dst + ".part"} {
				if err := os.WriteFile(path, []byte("existing"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			if err := c.DownloadToFile(context.Background(), "a", dst, DownloadOptions{Resume: true}); !errors.Is(err, want) {
				t.Fatal(err)
			}
			for _, path := range []string{dst, dst + ".part"} {
				data, err := os.ReadFile(path)
				if err != nil || string(data) != "existing" {
					t.Fatalf("modified file %s: %q %v", path, data, err)
				}
			}
			entries, err := os.ReadDir(dir)
			if err != nil || len(entries) != 2 || len(p.sessions) != 0 || len(p.uploads) != 0 {
				t.Fatalf("unexpected side effects: %v %v", entries, err)
			}
		})
	}
}

type noRead struct{ t *testing.T }

func (r noRead) Read([]byte) (int, error) {
	r.t.Error("unsupported operation read input")
	return 0, errors.New("unexpected read")
}
