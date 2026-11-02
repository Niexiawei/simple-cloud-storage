package cloudstorage

import (
	"errors"
	"fmt"
	"testing"
)

func TestErrorIs(t *testing.T) {
	t.Parallel()
	cause := fmt.Errorf("upstream failure")
	err := &Error{Op: "upload", Provider: "mock", Key: "a", Kind: ErrTemporary, Err: cause}
	if !errors.Is(err, ErrTemporary) {
		t.Fatal("Error does not match its kind")
	}
	if !errors.Is(err, cause) {
		t.Fatal("Error does not match its cause")
	}
	if errors.Is(err, ErrNotFound) {
		t.Fatal("Error matched an unrelated kind")
	}
}
