package statefile

import (
	"context"
	"errors"
	"os"
	"runtime"
	"testing"
)

func TestFailedReplacePreservesOldRecord(t *testing.T) {
	for _, name := range []string{"rename failure", "cancel before replace"} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			s, err := New(dir)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if err := s.Save(ctx, "key", "old"); err != nil {
				t.Fatal(err)
			}
			want := os.ErrPermission
			var replacement any = "new"
			if name == "rename failure" {
				s.renameFile = func(string, string) error { return os.ErrPermission }
			} else {
				replacement = cancellingState{cancel}
				want = context.Canceled
			}
			if err := s.Save(ctx, "key", replacement); !errors.Is(err, want) {
				t.Fatalf("Save error = %v", err)
			}
			reopened, err := New(dir)
			if err != nil {
				t.Fatal(err)
			}
			var value string
			if err := reopened.Load(context.Background(), "key", &value); err != nil || value != "old" {
				t.Fatalf("old record lost: %q %v", value, err)
			}
			entries, err := os.ReadDir(dir)
			if err != nil || len(entries) != 1 {
				t.Fatalf("temporary files remain: %v %v", entries, err)
			}
			if runtime.GOOS != "windows" {
				for _, path := range []string{dir, s.path("key")} {
					info, err := os.Stat(path)
					if err != nil {
						t.Fatal(err)
					}
					if info.Mode().Perm()&0077 != 0 {
						t.Fatalf("public permissions: %v", info.Mode())
					}
				}
			}
		})
	}
}

type cancellingState struct{ cancel context.CancelFunc }

func (s cancellingState) MarshalJSON() ([]byte, error) {
	s.cancel()
	return []byte(`"new"`), nil
}

func TestLoadClassifiesReadAndDecodeErrors(t *testing.T) {
	s, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	s.readFile = func(string) ([]byte, error) { return nil, os.ErrPermission }
	var data any
	if err := s.Load(context.Background(), "key", &data); !errors.Is(err, os.ErrPermission) || errors.Is(err, ErrInvalidData) {
		t.Fatalf("I/O error = %v", err)
	}
	s.readFile = func(string) ([]byte, error) { return []byte("{"), nil }
	if err := s.Load(context.Background(), "key", &data); !errors.Is(err, ErrInvalidData) {
		t.Fatalf("decode error = %v", err)
	}
}
