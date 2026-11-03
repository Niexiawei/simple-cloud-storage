package cloudstorage

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"
)

// activeTempUploads protects spool files used by Writers in this process.
var activeTempUploads sync.Map

// TempCleanupOptions controls explicit, non-recursive upload-spool maintenance.
type TempCleanupOptions struct {
	MinAge time.Duration // Must be positive; measured from the file's last modification.
	DryRun bool          // Only report candidates, without deleting them.
	Keep   []string      // Additional paths to retain, e.g. PendingUpload.ResumeSource().
	// Checkpoints must support CheckpointLister. Referenced upload sources are retained.
	// Supply every store sharing dir; use Keep for sources belonging to other stores.
	Checkpoints CheckpointStore
}

// TempFile describes a cleanup candidate. Removed is false for DryRun or a failed removal.
type TempFile struct {
	Path    string
	Size    int64
	ModTime time.Time
	Removed bool
}

// CleanupTempUploads removes only old .simple-cloud-storage-*.upload regular files
// immediately inside dir. It never follows symlinks or traverses child directories.
// Stop other processes using dir and the checkpoint stores before maintenance;
// age alone does not prove that a file is abandoned. Active Writers in this process
// are skipped. Include complete pending sources in Checkpoints or Keep.
// On error, the returned entries describe any work already performed.
func CleanupTempUploads(ctx context.Context, dir string, opts TempCleanupOptions) ([]TempFile, error) {
	if dir == "" || opts.MinAge <= 0 {
		return nil, ErrInvalidArgument
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	abs, err := filepath.Abs(dir)
	if err != nil {
		return nil, err
	}
	keep := make(map[string]bool, len(opts.Keep))
	for _, name := range opts.Keep {
		if name == "" {
			return nil, ErrInvalidArgument
		}
		keep[tempPathKey(name)] = true
	}
	if opts.Checkpoints != nil {
		lister, ok := opts.Checkpoints.(CheckpointLister)
		if !ok {
			return nil, ErrUnsupported
		}
		checkpoints, err := lister.List(ctx)
		if err != nil {
			return nil, err // Fail before deleting anything if progress cannot be read.
		}
		for _, cp := range checkpoints {
			if cp.Kind != CheckpointUpload {
				if _, err := cp.DecodeDownload(); err != nil {
					return nil, err
				}
				continue
			}
			state, err := cp.DecodeUpload()
			if err != nil {
				return nil, err
			}
			if state.Source != "" {
				keep[tempPathKey(state.Source)] = true
			}
		}
	}
	root, err := os.OpenRoot(abs)
	if err != nil {
		return nil, err
	}
	defer root.Close()
	f, err := root.Open(".")
	if err != nil {
		return nil, err
	}
	defer f.Close()
	cutoff := time.Now().Add(-opts.MinAge)
	var result []TempFile
	for {
		entries, readErr := f.ReadDir(100)
		for _, entry := range entries {
			if err := ctx.Err(); err != nil {
				return result, err
			}
			name := entry.Name()
			if !strings.HasPrefix(name, ".simple-cloud-storage-") || !strings.HasSuffix(name, ".upload") {
				continue
			}
			path := filepath.Join(abs, name)
			key := tempPathKey(path)
			if _, active := activeTempUploads.Load(key); active || keep[key] {
				continue
			}
			info, err := root.Lstat(name)
			if os.IsNotExist(err) {
				continue
			}
			if err != nil {
				return result, err
			}
			if !info.Mode().IsRegular() || !info.ModTime().Before(cutoff) {
				continue
			}
			candidate := TempFile{Path: path, Size: info.Size(), ModTime: info.ModTime()}
			if !opts.DryRun {
				if err := root.Remove(name); err != nil && !os.IsNotExist(err) {
					return append(result, candidate), err
				}
				candidate.Removed = true
			}
			result = append(result, candidate)
		}
		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				return result, nil
			}
			return result, readErr
		}
	}
}

func tempPathKey(path string) string {
	abs, err := filepath.Abs(path)
	if err == nil {
		path = abs
	}
	path = filepath.Clean(path)
	if runtime.GOOS == "windows" {
		path = strings.ToLower(path)
	}
	return path
}
