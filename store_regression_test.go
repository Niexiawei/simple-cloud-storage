package cloudstorage

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestFileStoresIsolatedAndReopened(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	cs, err := NewFileCheckpointStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	ts, err := NewFileTokenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	cp := testCheckpoint()
	if err := ts.Save(ctx, cp.ID, Token{AccessToken: "secret"}); err != nil {
		t.Fatal(err)
	}
	if err := cs.Save(ctx, cp); err != nil {
		t.Fatal(err)
	}
	cs, err = NewFileCheckpointStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	ts, err = NewFileTokenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	if token, err := ts.Load(ctx, cp.ID); err != nil || token.AccessToken != "secret" {
		t.Fatalf("token overwritten: %+v %v", token, err)
	}
	if _, err := cs.Load(ctx, cp.ID); err != nil {
		t.Fatal(err)
	}
	cp.Data = json.RawMessage(`{"updated":true}`)
	if err := cs.Save(ctx, cp); err != nil {
		t.Fatal(err)
	}
	if err := ts.Save(ctx, cp.ID, Token{AccessToken: "new"}); err != nil {
		t.Fatal(err)
	}
	if got, err := cs.Load(ctx, cp.ID); err != nil || string(got.Data) != string(cp.Data) {
		t.Fatalf("checkpoint overwritten: %+v %v", got, err)
	}
	if err := cs.Delete(ctx, cp.ID); err != nil {
		t.Fatal(err)
	}
	if token, err := ts.Load(ctx, cp.ID); err != nil || token.AccessToken != "new" {
		t.Fatalf("token deleted: %+v %v", token, err)
	}
	if err := cs.Save(ctx, cp); err != nil {
		t.Fatal(err)
	}
	if err := ts.Delete(ctx, cp.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := cs.Load(ctx, cp.ID); err != nil {
		t.Fatal(err)
	}
}

func TestCheckpointEnvelopeRejectsInvalidRecords(t *testing.T) {
	for _, name := range []string{"missing data", "null", "array", "bad json", "empty file", "old version", "unknown version", "wrong id", "wrong path", "wrong kind", "missing account"} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			s, err := NewFileCheckpointStore(dir)
			if err != nil {
				t.Fatal(err)
			}
			cp := testCheckpoint()
			id := cp.ID
			switch name {
			case "missing data":
				cp.Data = nil
			case "null":
				cp.Data = json.RawMessage(`null`)
			case "array":
				cp.Data = json.RawMessage(`[]`)
			case "old version":
				cp.Version = 0
			case "unknown version":
				cp.Version = 999
			case "wrong id":
				cp.ID = "wrong"
			case "wrong path":
				cp.Path = "../escape"
			case "wrong kind":
				cp.Kind = "other"
			case "missing account":
				cp.AccountID = ""
			}
			data, err := json.Marshal(cp)
			if err != nil {
				t.Fatal(err)
			}
			if name == "bad json" {
				data = []byte(`{"id":`)
			}
			if name == "empty file" {
				data = nil
			}
			path := hashedStatePath(filepath.Join(dir, "checkpoints"), id)
			if err := os.WriteFile(path, data, 0600); err != nil {
				t.Fatal(err)
			}
			if _, err := s.Load(context.Background(), id); !errors.Is(err, ErrInvalidCheckpoint) || errors.Is(err, ErrCheckpointNotFound) {
				t.Fatalf("Load error = %v", err)
			}
			if got, err := os.ReadFile(path); err != nil || string(got) != string(data) {
				t.Fatal("invalid record was modified")
			}
			if name != "bad json" && name != "empty file" {
				if err := s.Save(context.Background(), cp); !errors.Is(err, ErrInvalidCheckpoint) {
					t.Fatalf("Save error = %v", err)
				}
			}
		})
	}
}

func TestCheckpointFilesystemErrorIsNotCorruption(t *testing.T) {
	dir := t.TempDir()
	s, err := NewFileCheckpointStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	// Reading a directory as a file produces a real filesystem error on Windows and Unix.
	if err := os.Mkdir(hashedStatePath(filepath.Join(dir, "checkpoints"), "io-error"), 0700); err != nil {
		t.Fatal(err)
	}
	_, err = s.Load(context.Background(), "io-error")
	var pathErr *os.PathError
	if err == nil || !errors.As(err, &pathErr) || errors.Is(err, ErrInvalidCheckpoint) || errors.Is(err, ErrCheckpointNotFound) {
		t.Fatalf("misclassified I/O error: %v", err)
	}
}

func TestLegacyStateIsNotRead(t *testing.T) {
	dir := t.TempDir()
	cp := testCheckpoint()
	if err := os.WriteFile(hashedStatePath(dir, cp.ID), []byte(`{"access_token":"legacy"}`), 0600); err != nil {
		t.Fatal(err)
	}
	cs, err := NewFileCheckpointStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	ts, err := NewFileTokenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cs.Load(context.Background(), cp.ID); !errors.Is(err, ErrCheckpointNotFound) {
		t.Fatal(err)
	}
	if _, err := ts.Load(context.Background(), cp.ID); !errors.Is(err, ErrTokenNotFound) {
		t.Fatal(err)
	}
}
