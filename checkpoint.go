package cloudstorage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/Niexiawei/simple-cloud-storage/internal/statefile"
)

var ErrCheckpointNotFound = errors.New("checkpoint not found")

// Checkpoint contains resumable operation state.
type Checkpoint struct {
	Version   int            `json:"version"`
	Kind      CheckpointKind `json:"kind"`
	Provider  string         `json:"provider"`
	AccountID string         `json:"account_id"`
	// Path is internal cloud identity metadata. Storage callers always use logical keys.
	Path      string          `json:"path"`
	ResumeID  string          `json:"resume_id,omitempty"`
	ID        string          `json:"id"`
	Data      json.RawMessage `json:"data"`
	UpdatedAt time.Time       `json:"updated_at"`
}

// CheckpointStore persists checkpoints by their string ID.
type CheckpointStore interface {
	Load(ctx context.Context, id string) (Checkpoint, error)
	Save(ctx context.Context, checkpoint Checkpoint) error
	Delete(ctx context.Context, id string) error
}

type fileCheckpointStore struct {
	store *statefile.Store
}

// NewFileCheckpointStore stores versioned records in dir/checkpoints.
// Legacy records directly in dir must be explicitly recreated; there is no fallback.
func NewFileCheckpointStore(dir string) (CheckpointStore, error) {
	if dir == "" {
		return nil, fmt.Errorf("checkpoint directory: %w", ErrInvalidArgument)
	}
	store, err := statefile.New(filepath.Join(dir, "checkpoints"))
	if err != nil {
		return nil, err
	}
	return &fileCheckpointStore{store: store}, nil
}

func (s *fileCheckpointStore) Load(ctx context.Context, id string) (Checkpoint, error) {
	if id == "" {
		return Checkpoint{}, fmt.Errorf("load checkpoint: %w: empty ID", ErrInvalidArgument)
	}
	var checkpoint Checkpoint
	if err := s.store.Load(ctx, id, &checkpoint); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return Checkpoint{}, ErrCheckpointNotFound
		}
		if errors.Is(err, statefile.ErrInvalidData) {
			return Checkpoint{}, fmt.Errorf("load checkpoint: %w", errors.Join(ErrInvalidCheckpoint, err))
		}
		return Checkpoint{}, fmt.Errorf("load checkpoint: %w", err)
	}
	if checkpoint.ID != id {
		return Checkpoint{}, fmt.Errorf("load checkpoint: %w: stored ID does not match requested ID", ErrInvalidCheckpoint)
	}
	if err := checkpoint.Validate(); err != nil {
		return Checkpoint{}, fmt.Errorf("load checkpoint: %w", err)
	}
	return checkpoint, nil
}

func (s *fileCheckpointStore) Save(ctx context.Context, checkpoint Checkpoint) error {
	if checkpoint.ID == "" {
		return fmt.Errorf("save checkpoint: %w: empty ID", ErrInvalidArgument)
	}
	if err := checkpoint.Validate(); err != nil {
		return fmt.Errorf("save checkpoint: %w", err)
	}
	if checkpoint.UpdatedAt.IsZero() {
		checkpoint.UpdatedAt = time.Now().UTC()
	}
	if err := s.store.Save(ctx, checkpoint.ID, checkpoint); err != nil {
		return fmt.Errorf("save checkpoint: %w", err)
	}
	return nil
}

func (s *fileCheckpointStore) Delete(ctx context.Context, id string) error {
	if id == "" {
		return fmt.Errorf("delete checkpoint: %w: empty ID", ErrInvalidArgument)
	}
	if err := s.store.Delete(ctx, id); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return ErrCheckpointNotFound
		}
		return fmt.Errorf("delete checkpoint: %w", err)
	}
	return nil
}
