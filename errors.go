package cloudstorage

import (
	"errors"
	"fmt"
	"time"
)

var (
	ErrNotFound          = errors.New("cloud storage: not found")
	ErrPermission        = errors.New("cloud storage: permission denied")
	ErrAuthentication    = errors.New("cloud storage: authentication failed")
	ErrConflict          = errors.New("cloud storage: conflict")
	ErrRateLimited       = errors.New("cloud storage: rate limited")
	ErrTemporary         = errors.New("cloud storage: temporary failure")
	ErrUnsupported       = errors.New("cloud storage: unsupported operation")
	ErrInvalidCheckpoint = errors.New("cloud storage: invalid checkpoint")
	ErrInvalidArgument   = errors.New("cloud storage: invalid argument")
	ErrWriterClosed      = errors.New("cloud storage: writer closed")
	ErrWriterAborted     = errors.New("cloud storage: writer aborted")
	ErrSizeMismatch      = errors.New("cloud storage: size mismatch")
	ErrProtocol          = errors.New("cloud storage: invalid provider protocol")
	ErrSessionExpired    = errors.New("cloud storage: upload session expired")
)

// Error adds operation and provider context while preserving errors.Is checks.
type Error struct {
	Op         string
	Provider   string
	Key        string
	Kind       error
	HTTPStatus int
	RetryAfter time.Duration
	Err        error
}

func (e *Error) Error() string {
	if e == nil {
		return "<nil>"
	}
	message := "cloud storage"
	if e.Op != "" {
		message += " " + e.Op
	}
	if e.Provider != "" {
		message += " provider=" + e.Provider
	}
	if e.Key != "" {
		message += fmt.Sprintf(" key=%q", e.Key)
	}
	if e.Kind != nil {
		message += ": " + e.Kind.Error()
	}
	if e.Err != nil {
		message += ": " + e.Err.Error()
	}
	return message
}

func (e *Error) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

func (e *Error) Is(target error) bool {
	if e == nil {
		return false
	}
	return target == e.Kind || errors.Is(e.Err, target)
}
