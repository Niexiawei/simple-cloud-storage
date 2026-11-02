package cloudstorage

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/Niexiawei/simple-cloud-storage/internal/statefile"
)

var ErrTokenNotFound = errors.New("token not found")

// Token contains OAuth credentials and provider-specific token metadata.
type Token struct {
	AccessToken  string            `json:"access_token"`
	RefreshToken string            `json:"refresh_token"`
	TokenType    string            `json:"token_type"`
	Expiry       time.Time         `json:"expiry"`
	Extra        map[string]string `json:"extra,omitempty"`
}

// TokenStore persists tokens by account.
type TokenStore interface {
	Load(ctx context.Context, account string) (Token, error)
	Save(ctx context.Context, account string, token Token) error
	Delete(ctx context.Context, account string) error
}

type fileTokenStore struct {
	store *statefile.Store
}

// NewFileTokenStore stores credentials in dir/tokens with hashed filenames.
// Legacy records directly in dir are not loaded automatically.
func NewFileTokenStore(dir string) (TokenStore, error) {
	if dir == "" {
		return nil, fmt.Errorf("token directory: %w", ErrInvalidArgument)
	}
	store, err := statefile.New(filepath.Join(dir, "tokens"))
	if err != nil {
		return nil, err
	}
	return &fileTokenStore{store: store}, nil
}

func (s *fileTokenStore) Load(ctx context.Context, account string) (Token, error) {
	if account == "" {
		return Token{}, fmt.Errorf("load token: %w: empty account", ErrInvalidArgument)
	}
	var token Token
	if err := s.store.Load(ctx, account, &token); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return Token{}, ErrTokenNotFound
		}
		return Token{}, fmt.Errorf("load token: %w", err)
	}
	return token, nil
}

func (s *fileTokenStore) Save(ctx context.Context, account string, token Token) error {
	if account == "" {
		return fmt.Errorf("save token: %w: empty account", ErrInvalidArgument)
	}
	if err := s.store.Save(ctx, account, token); err != nil {
		return fmt.Errorf("save token: %w", err)
	}
	return nil
}

func (s *fileTokenStore) Delete(ctx context.Context, account string) error {
	if account == "" {
		return fmt.Errorf("delete token: %w: empty account", ErrInvalidArgument)
	}
	if err := s.store.Delete(ctx, account); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return ErrTokenNotFound
		}
		return fmt.Errorf("delete token: %w", err)
	}
	return nil
}
