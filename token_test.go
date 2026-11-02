package cloudstorage

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestFileTokenStoreSaveLoadUpdateDelete(t *testing.T) {
	dir := t.TempDir()
	store, err := NewFileTokenStore(dir)
	if err != nil {
		t.Fatalf("NewFileTokenStore() error = %v", err)
	}
	ctx := context.Background()
	account := "user@example.com"
	token := Token{
		AccessToken:  "access-token-one",
		RefreshToken: "refresh-token-one",
		TokenType:    "Bearer",
		Expiry:       time.Date(2026, time.November, 2, 16, 0, 0, 0, time.UTC),
		Extra:        map[string]string{"scope": "read", "driver": "example"},
	}

	if err := store.Save(ctx, account, token); err != nil {
		t.Fatalf("Save() error = %v", err)
	}
	store, err = NewFileTokenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	loaded, err := store.Load(ctx, account)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	assertTokenEqual(t, loaded, token)

	token.AccessToken = "access-token-two"
	token.Extra["scope"] = "read write"
	if err := store.Save(ctx, account, token); err != nil {
		t.Fatalf("Save() update error = %v", err)
	}
	loaded, err = store.Load(ctx, account)
	if err != nil {
		t.Fatalf("Load() after update error = %v", err)
	}
	assertTokenEqual(t, loaded, token)

	if err := store.Delete(ctx, account); err != nil {
		t.Fatalf("Delete() error = %v", err)
	}
	if _, err := store.Load(ctx, account); !errors.Is(err, ErrTokenNotFound) {
		t.Fatalf("Load() after Delete error = %v, want ErrTokenNotFound", err)
	}
	if err := store.Delete(ctx, account); !errors.Is(err, ErrTokenNotFound) {
		t.Fatalf("Delete() missing error = %v, want ErrTokenNotFound", err)
	}
}

func TestFileTokenStoreValidatesAccount(t *testing.T) {
	store, err := NewFileTokenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := store.Save(ctx, "", Token{}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("Save(empty account) error = %v", err)
	}
	if _, err := store.Load(ctx, ""); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("Load(empty account) error = %v", err)
	}
	if err := store.Delete(ctx, ""); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("Delete(empty account) error = %v", err)
	}
}

func TestFileTokenStoreStableFilenameHidesSecrets(t *testing.T) {
	dir := t.TempDir()
	store, err := NewFileTokenStore(dir)
	if err != nil {
		t.Fatalf("NewFileTokenStore() error = %v", err)
	}
	account := "../accounts/user@example.com"
	token := Token{
		AccessToken:  "highly-sensitive-access-token",
		RefreshToken: "highly-sensitive-refresh-token",
		TokenType:    "Bearer",
	}
	if err := store.Save(context.Background(), account, token); err != nil {
		t.Fatalf("Save() error = %v", err)
	}
	if err := store.Save(context.Background(), account, token); err != nil {
		t.Fatalf("Save() update error = %v", err)
	}

	sum := sha256.Sum256([]byte(account))
	expectedName := hex.EncodeToString(sum[:]) + ".json"
	entries, err := os.ReadDir(filepath.Join(dir, "tokens"))
	if err != nil {
		t.Fatalf("ReadDir() error = %v", err)
	}
	if len(entries) != 1 || entries[0].Name() != expectedName {
		t.Fatalf("state files = %v, want only %q", entryNames(entries), expectedName)
	}
	for _, entry := range entries {
		name := entry.Name()
		for _, secret := range []string{account, token.AccessToken, token.RefreshToken} {
			if strings.Contains(name, secret) {
				t.Fatalf("filename %q contains sensitive value %q", name, secret)
			}
		}
	}
}

func TestFileTokenStoreCorruptData(t *testing.T) {
	dir := t.TempDir()
	store, err := NewFileTokenStore(dir)
	if err != nil {
		t.Fatalf("NewFileTokenStore() error = %v", err)
	}
	account := "corrupt-token"
	if err := store.Save(context.Background(), account, Token{AccessToken: "secret"}); err != nil {
		t.Fatalf("Save() error = %v", err)
	}
	path := hashedStatePath(filepath.Join(dir, "tokens"), account)
	if err := os.WriteFile(path, []byte(`not-json`), 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	if _, err := store.Load(context.Background(), account); err == nil {
		t.Fatal("Load() error = nil, want JSON corruption error")
	} else if errors.Is(err, ErrTokenNotFound) {
		t.Fatalf("Load() error = %v, must not be ErrTokenNotFound", err)
	}
}

func TestFileTokenStoreChecksContext(t *testing.T) {
	store, err := NewFileTokenStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewFileTokenStore() error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := store.Save(ctx, "account", Token{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("Save() error = %v, want context.Canceled", err)
	}
	if _, err := store.Load(ctx, "account"); !errors.Is(err, context.Canceled) {
		t.Fatalf("Load() error = %v, want context.Canceled", err)
	}
	if err := store.Delete(ctx, "account"); !errors.Is(err, context.Canceled) {
		t.Fatalf("Delete() error = %v, want context.Canceled", err)
	}
}

func assertTokenEqual(t *testing.T, got, want Token) {
	t.Helper()
	if got.AccessToken != want.AccessToken ||
		got.RefreshToken != want.RefreshToken ||
		got.TokenType != want.TokenType ||
		!got.Expiry.Equal(want.Expiry) ||
		!reflect.DeepEqual(got.Extra, want.Extra) {
		t.Fatalf("token = %#v, want %#v", got, want)
	}
}
