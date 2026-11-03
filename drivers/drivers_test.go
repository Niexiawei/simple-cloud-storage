package drivers

import (
	"context"
	"errors"
	"testing"

	cloudstorage "github.com/Niexiawei/simple-cloud-storage"
)

func TestUnsupportedRuntimeDriver(t *testing.T) {
	t.Setenv("CLOUD_STORAGE_PROVIDER", "unimplemented")
	if _, err := Connect(context.Background(), Config{}); !errors.Is(err, cloudstorage.ErrUnsupported) {
		t.Fatal(err)
	}
}
