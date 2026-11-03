package cloudstorage

import (
	"context"
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
)

func (c *Client) downloadResumable(ctx context.Context, key, dst string, opts DownloadOptions) error {
	if dst == "" {
		return c.wrapError("download", key, ErrInvalidArgument)
	}
	info, err := c.Stat(ctx, key)
	if err != nil {
		return err
	}
	target, err := filepath.Abs(dst)
	if err != nil {
		return err
	}
	state := DownloadState{FileID: info.ID, Size: info.Size, MD5: info.MD5, SHA1: info.SHA1, SHA256: info.SHA256,
		ETag: info.ETag, ModTime: info.ModTime, Target: target, Partial: target + ".part"}
	if err := state.Validate(); err != nil {
		return err
	}
	data, err := json.Marshal(state)
	if err != nil {
		return err
	}
	cp, err := c.newCheckpoint(ctx, key, CheckpointDownload, "", data)
	if err != nil {
		return err
	}
	store := c.config.CheckpointStore
	old, err := store.Load(ctx, cp.ID)
	if err == nil {
		if err := old.Validate(); err != nil {
			return err
		}
		state, err = old.DecodeDownload()
		if err != nil {
			return err
		}
		if old.ID != cp.ID || old.Path != cp.Path || old.Provider != cp.Provider || old.AccountID != cp.AccountID {
			return ErrInvalidCheckpoint
		}
		if err := state.MatchObject(info, target); err != nil {
			return err
		}
	} else if !errors.Is(err, ErrCheckpointNotFound) {
		return err
	}
	f, err := os.OpenFile(state.Partial, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return err
	}
	defer f.Close()
	local, err := f.Stat()
	if err != nil {
		return err
	}
	state.Offset = min(state.Offset, local.Size())
	if err := f.Truncate(state.Offset); err != nil {
		return err
	}
	if _, err := f.Seek(state.Offset, io.SeekStart); err != nil {
		return err
	}
	save := func() error {
		if err := f.Sync(); err != nil {
			return err
		}
		data, err := json.Marshal(state)
		if err != nil {
			return err
		}
		cp.Data, cp.UpdatedAt = data, time.Now().UTC()
		return store.Save(ctx, cp)
	}
	if err := save(); err != nil {
		return err
	}
	if opts.Progress != nil {
		opts.Progress(Progress{Transferred: state.Offset, Total: state.Size})
	}
	if state.Offset < state.Size {
		r, err := c.Open(ctx, key, ReadOptions{Offset: state.Offset})
		if err != nil {
			return err
		}
		buffer := make([]byte, 1<<20)
		for state.Offset < state.Size {
			want := min(int64(len(buffer)), state.Size-state.Offset)
			n, readErr := io.ReadFull(&contextReader{ctx: ctx, r: r}, buffer[:want])
			if n > 0 {
				written, writeErr := f.Write(buffer[:n])
				if writeErr != nil {
					r.Close()
					return writeErr
				}
				if written != n {
					r.Close()
					return io.ErrShortWrite
				}
				state.Offset += int64(n)
				if err := save(); err != nil {
					r.Close()
					return err
				}
				if opts.Progress != nil {
					opts.Progress(Progress{Transferred: state.Offset, Total: state.Size})
				}
			}
			if readErr != nil {
				return errors.Join(readErr, r.Close())
			}
		}
		// A validated provider Reader must end at the expected object boundary.
		var extra [1]byte
		n, readErr := r.Read(extra[:])
		closeErr := r.Close()
		if n != 0 || !errors.Is(readErr, io.EOF) {
			return errors.Join(ErrSizeMismatch, readErr, closeErr)
		}
		if closeErr != nil {
			return closeErr
		}
	}
	current, err := c.Stat(ctx, key)
	if err != nil {
		return err
	}
	if err := state.MatchObject(current, target); err != nil {
		return err
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	if err := verifyLocalHashes(ctx, f, info); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := replaceLocalFile(state.Partial, target); err != nil {
		return err
	}
	if err := store.Delete(ctx, cp.ID); err != nil && !errors.Is(err, ErrCheckpointNotFound) {
		return err
	}
	return nil
}

func verifyLocalHashes(ctx context.Context, r io.Reader, info ObjectInfo) error {
	m, s1, s256 := md5.New(), sha1.New(), sha256.New()
	n, err := io.Copy(io.MultiWriter(m, s1, s256), &contextReader{ctx: ctx, r: r})
	if err != nil {
		return err
	}
	if n != info.Size {
		return ErrSizeMismatch
	}
	for _, pair := range [][2]string{{info.MD5, hex.EncodeToString(m.Sum(nil))}, {info.SHA1, hex.EncodeToString(s1.Sum(nil))}, {info.SHA256, hex.EncodeToString(s256.Sum(nil))}} {
		if pair[0] != "" && !strings.EqualFold(pair[0], pair[1]) {
			return fmt.Errorf("%w: download hash mismatch", ErrSizeMismatch)
		}
	}
	return nil
}
