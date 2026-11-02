package statefile

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// Store persists one JSON document per string key in a directory.
type Store struct {
	dir        string
	readFile   func(string) ([]byte, error)
	renameFile func(string, string) error
}

// ErrInvalidData distinguishes decoding failures from filesystem failures.
var ErrInvalidData = errors.New("invalid state data")

func New(dir string) (*Store, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("create state directory: %w", err)
	}
	if err := os.Chmod(dir, 0o700); err != nil {
		return nil, fmt.Errorf("set state directory permissions: %w", err)
	}
	return &Store{dir: dir, readFile: os.ReadFile, renameFile: os.Rename}, nil
}

func (s *Store) Load(ctx context.Context, key string, dst any) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	data, err := s.readFile(s.path(key))
	if err != nil {
		return err
	}
	if err := json.Unmarshal(data, dst); err != nil {
		return fmt.Errorf("decode state file: %w", errors.Join(ErrInvalidData, err))
	}
	return nil
}

func (s *Store) Save(ctx context.Context, key string, src any) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	data, err := json.Marshal(src)
	if err != nil {
		return fmt.Errorf("encode state file: %w", err)
	}

	f, err := os.CreateTemp(s.dir, ".state-*.tmp")
	if err != nil {
		return fmt.Errorf("create temporary state file: %w", err)
	}
	tmp := f.Name()
	defer func() {
		_ = f.Close()
		_ = os.Remove(tmp)
	}()

	if err := f.Chmod(0o600); err != nil {
		return fmt.Errorf("set state file permissions: %w", err)
	}
	if _, err := f.Write(data); err != nil {
		return fmt.Errorf("write temporary state file: %w", err)
	}
	if err := f.Sync(); err != nil {
		return fmt.Errorf("sync temporary state file: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("close temporary state file: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := s.renameFile(tmp, s.path(key)); err != nil {
		return fmt.Errorf("replace state file: %w", err)
	}
	return nil
}

func (s *Store) Delete(ctx context.Context, key string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return os.Remove(s.path(key))
}

func (s *Store) path(key string) string {
	sum := sha256.Sum256([]byte(key))
	return filepath.Join(s.dir, hex.EncodeToString(sum[:])+".json")
}
