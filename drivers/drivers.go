// Package drivers selects a built-in provider using runtime configuration.
package drivers

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"

	cloudstorage "github.com/Niexiawei/simple-cloud-storage"
	"github.com/Niexiawei/simple-cloud-storage/quark"
)

type Config struct {
	Provider string
	Storage  cloudstorage.Config
	Quark    quark.Config
}
type Client interface {
	cloudstorage.Storage
	io.Closer
}

// Connect defaults Provider to CLOUD_STORAGE_PROVIDER, then quark. Credential
// environment variables are evaluated on connection, requiring no recompilation.
func Connect(ctx context.Context, cfg Config) (Client, error) {
	name := cfg.Provider
	if name == "" {
		name = os.Getenv("CLOUD_STORAGE_PROVIDER")
	}
	if name == "" {
		name = "quark"
	}
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "quark":
		return quark.Connect(ctx, cfg.Quark, cfg.Storage)
	default:
		return nil, fmt.Errorf("driver is not implemented: %w", cloudstorage.ErrUnsupported)
	}
}
