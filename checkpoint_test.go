package cloudstorage

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestFileCheckpointStoreSaveLoadUpdateDelete(t *testing.T) {
	dir := t.TempDir()
	store, err := NewFileCheckpointStore(dir)
	if err != nil {
		t.Fatalf("NewFileCheckpointStore() error = %v", err)
	}
	ctx := context.Background()
	updatedAt := time.Date(2026, time.November, 2, 15, 0, 0, 0, time.UTC)
	checkpoint := testCheckpoint()
	checkpoint.Data = json.RawMessage(`{"part":1,"etag":"first"}`)
	checkpoint.UpdatedAt = updatedAt

	if err := store.Save(ctx, checkpoint); err != nil {
		t.Fatalf("Save() error = %v", err)
	}
	store, err = NewFileCheckpointStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	loaded, err := store.Load(ctx, checkpoint.ID)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	assertCheckpointEqual(t, loaded, checkpoint)

	checkpoint.Data = json.RawMessage(`{"part":2,"etag":"second"}`)
	checkpoint.UpdatedAt = updatedAt.Add(time.Minute)
	if err := store.Save(ctx, checkpoint); err != nil {
		t.Fatalf("Save() update error = %v", err)
	}
	loaded, err = store.Load(ctx, checkpoint.ID)
	if err != nil {
		t.Fatalf("Load() after update error = %v", err)
	}
	assertCheckpointEqual(t, loaded, checkpoint)

	if err := store.Delete(ctx, checkpoint.ID); err != nil {
		t.Fatalf("Delete() error = %v", err)
	}
	if _, err := store.Load(ctx, checkpoint.ID); !errors.Is(err, ErrCheckpointNotFound) {
		t.Fatalf("Load() after Delete error = %v, want ErrCheckpointNotFound", err)
	}
	if err := store.Delete(ctx, checkpoint.ID); !errors.Is(err, ErrCheckpointNotFound) {
		t.Fatalf("Delete() missing error = %v, want ErrCheckpointNotFound", err)
	}
}

func TestFileCheckpointStoreStableFilename(t *testing.T) {
	dir := t.TempDir()
	store, err := NewFileCheckpointStore(dir)
	if err != nil {
		t.Fatalf("NewFileCheckpointStore() error = %v", err)
	}
	checkpoint := testCheckpoint()
	checkpoint.ResumeID = "../account/upload/id?with=reserved#characters"
	checkpoint.ID = checkpoint.fingerprint()
	id := checkpoint.ID
	if err := store.Save(context.Background(), checkpoint); err != nil {
		t.Fatalf("Save() error = %v", err)
	}
	checkpoint.Data = json.RawMessage(`{"version":2}`)
	if err := store.Save(context.Background(), checkpoint); err != nil {
		t.Fatalf("Save() update error = %v", err)
	}

	sum := sha256.Sum256([]byte(id))
	expectedName := hex.EncodeToString(sum[:]) + ".json"
	if _, err := os.Stat(filepath.Join(dir, "checkpoints", expectedName)); err != nil {
		t.Fatalf("stable hashed state file: %v", err)
	}
	entries, err := os.ReadDir(filepath.Join(dir, "checkpoints"))
	if err != nil {
		t.Fatalf("ReadDir() error = %v", err)
	}
	if len(entries) != 1 || entries[0].Name() != expectedName {
		t.Fatalf("state files = %v, want only %q", entryNames(entries), expectedName)
	}
}

func TestFileCheckpointStoreCorruptData(t *testing.T) {
	dir := t.TempDir()
	store, err := NewFileCheckpointStore(dir)
	if err != nil {
		t.Fatalf("NewFileCheckpointStore() error = %v", err)
	}
	cp := testCheckpoint()
	id := cp.ID
	if err := store.Save(context.Background(), cp); err != nil {
		t.Fatalf("Save() error = %v", err)
	}
	path := hashedStatePath(filepath.Join(dir, "checkpoints"), id)
	if err := os.WriteFile(path, []byte(`{"id":`), 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	if _, err := store.Load(context.Background(), id); err == nil {
		t.Fatal("Load() error = nil, want JSON corruption error")
	} else if errors.Is(err, ErrCheckpointNotFound) {
		t.Fatalf("Load() error = %v, must not be ErrCheckpointNotFound", err)
	} else if !errors.Is(err, ErrInvalidCheckpoint) {
		t.Fatalf("Load() error = %v, want ErrInvalidCheckpoint", err)
	}
}

func TestFileCheckpointStoreRejectsMismatchedID(t *testing.T) {
	dir := t.TempDir()
	store, err := NewFileCheckpointStore(dir)
	if err != nil {
		t.Fatalf("NewFileCheckpointStore() error = %v", err)
	}
	requested := "expected"
	data, err := json.Marshal(Checkpoint{ID: "different", Data: json.RawMessage(`{}`)})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(hashedStatePath(filepath.Join(dir, "checkpoints"), requested), data, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Load(context.Background(), requested); !errors.Is(err, ErrInvalidCheckpoint) {
		t.Fatalf("Load() error = %v, want ErrInvalidCheckpoint", err)
	}
}

func TestFileCheckpointStoreValidatesInput(t *testing.T) {
	store, err := NewFileCheckpointStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := store.Save(ctx, Checkpoint{Data: json.RawMessage(`{}`)}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("Save(empty ID) error = %v", err)
	}
	if err := store.Save(ctx, Checkpoint{ID: "id", Data: json.RawMessage(`{`)}); !errors.Is(err, ErrInvalidCheckpoint) {
		t.Fatalf("Save(invalid data) error = %v", err)
	}
	if _, err := store.Load(ctx, ""); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("Load(empty ID) error = %v", err)
	}
	if err := store.Delete(ctx, ""); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("Delete(empty ID) error = %v", err)
	}
}

func TestFileCheckpointStoreChecksContext(t *testing.T) {
	store, err := NewFileCheckpointStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewFileCheckpointStore() error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	checkpoint := testCheckpoint()
	if err := store.Save(ctx, checkpoint); !errors.Is(err, context.Canceled) {
		t.Fatalf("Save() error = %v, want context.Canceled", err)
	}
	if _, err := store.Load(ctx, checkpoint.ID); !errors.Is(err, context.Canceled) {
		t.Fatalf("Load() error = %v, want context.Canceled", err)
	}
	if err := store.Delete(ctx, checkpoint.ID); !errors.Is(err, context.Canceled) {
		t.Fatalf("Delete() error = %v, want context.Canceled", err)
	}
}

func testCheckpoint() Checkpoint {
	cp := Checkpoint{Version: CheckpointVersion, Kind: CheckpointUpload, Provider: "mock", AccountID: "account", Path: "apps/test/a", Data: json.RawMessage(`{}`)}
	cp.ID = cp.fingerprint()
	return cp
}

func assertCheckpointEqual(t *testing.T, got, want Checkpoint) {
	t.Helper()
	if got.ID != want.ID || !bytes.Equal(got.Data, want.Data) || !got.UpdatedAt.Equal(want.UpdatedAt) {
		t.Fatalf("checkpoint = %#v, want %#v", got, want)
	}
}

func hashedStatePath(dir, key string) string {
	sum := sha256.Sum256([]byte(key))
	return filepath.Join(dir, hex.EncodeToString(sum[:])+".json")
}

func entryNames(entries []os.DirEntry) []string {
	names := make([]string, len(entries))
	for i, entry := range entries {
		names[i] = entry.Name()
	}
	return names
}
