package cloudstorage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"reflect"
	"strings"
	"testing"
)

func TestWriterBoundariesAndProgress(t *testing.T) {
	for _, unknown := range []bool{false, true} {
		for _, size := range []int{0, 3, 4, 5, 8, 10} {
			t.Run(fmt.Sprintf("unknown=%v size=%d", unknown, size), func(t *testing.T) {
				p := newMockProvider()
				c := testClient(t, p)
				var progress []Progress
				opts := WriterOptions{Size: sizePointer(int64(size)), Progress: func(p Progress) { progress = append(progress, p) }}
				if unknown {
					opts.Size = nil
				}
				w, err := c.NewWriter(context.Background(), "a", opts)
				if err != nil {
					t.Fatal(err)
				}
				data := strings.Repeat("x", size)
				if n, err := io.WriteString(w, data); err != nil || n != size {
					t.Fatalf("Write = %d %v", n, err)
				}
				if err := w.Close(); err != nil {
					t.Fatal(err)
				}
				if err := w.Close(); err != nil {
					t.Fatal(err)
				}
				if _, err := w.Write([]byte("x")); !errors.Is(err, ErrWriterClosed) {
					t.Fatal(err)
				}
				if string(p.objects["apps/test/a"]) != data {
					t.Fatal("wrong uploaded data")
				}
				if size >= 4 {
					var want []int64
					for remain := size; remain > 0; remain -= 4 {
						want = append(want, int64(min(4, remain)))
					}
					if !reflect.DeepEqual(p.sessions[0].sizes, want) {
						t.Fatalf("parts=%v want=%v", p.sessions[0].sizes, want)
					}
				} else if len(p.uploads) != 1 || len(p.sessions) != 0 {
					t.Fatal("incorrect threshold mode")
				}
				var previous int64
				for _, p := range progress {
					if p.Total != int64(size) || p.Transferred < previous || p.Transferred > p.Total {
						t.Fatalf("invalid progress %+v", progress)
					}
					previous = p.Transferred
				}
				if size > 0 && previous != int64(size) {
					t.Fatalf("incomplete progress %+v", progress)
				}
				entries, err := os.ReadDir(c.config.TempDir)
				if err != nil || len(entries) != 0 {
					t.Fatalf("spool leak %v %v", entries, err)
				}
			})
		}
	}
}

func TestWriterOverrunAndPartialFailure(t *testing.T) {
	for _, name := range []string{"overrun", "part failure", "short spool write", "complete failure"} {
		t.Run(name, func(t *testing.T) {
			p := newMockProvider()
			c := testClient(t, p)
			opts := WriterOptions{Size: sizePointer(10)}
			if name == "short spool write" {
				opts.Size = nil
			}
			w, err := c.NewWriter(context.Background(), "a", opts)
			if err != nil {
				t.Fatal(err)
			}
			var want error
			switch name {
			case "overrun":
				want = ErrSizeMismatch
				if n, err := w.Write([]byte("too-many-bytes")); n != 0 || !errors.Is(err, want) {
					t.Fatalf("Write = %d %v", n, err)
				}
			case "part failure":
				p.failPart = 2
				want = ErrTemporary
				if n, err := w.Write([]byte("abcdefghij")); n != 8 || !errors.Is(err, want) {
					t.Fatalf("Write = %d %v", n, err)
				}
			case "short spool write":
				want = io.ErrShortWrite
				w.(*uploadWriter).spool = shortSpoolWriter{}
				if n, err := w.Write([]byte("abc")); n != 2 || !errors.Is(err, want) {
					t.Fatalf("Write = %d %v", n, err)
				}
			case "complete failure":
				want = ErrTemporary
				p.completeErr = want
				if _, err := w.Write([]byte("abcdefghij")); err != nil {
					t.Fatal(err)
				}
			}
			first := w.Close()
			if !errors.Is(first, want) {
				t.Fatalf("Close = %v", first)
			}
			if err := w.Close(); err != first {
				t.Fatalf("Close lost original error: %v", err)
			}
			if w.Info().ID != "" || len(p.objects) != 0 {
				t.Fatal("failure reported as success")
			}
			if len(p.sessions) > 0 && !p.sessions[0].aborted {
				t.Fatal("remote session not aborted")
			}
			entries, err := os.ReadDir(c.config.TempDir)
			if err != nil || len(entries) != 0 {
				t.Fatalf("spool leak %v %v", entries, err)
			}
		})
	}
}

type shortSpoolWriter struct{}

func (shortSpoolWriter) Write(p []byte) (int, error) { return len(p) - 1, nil }
