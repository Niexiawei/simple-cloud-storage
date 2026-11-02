package cloudstorage

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/Niexiawei/simple-cloud-storage/internal/pathx"
)

// CheckpointVersion is the only currently supported on-disk envelope version.
const CheckpointVersion = 1

// CheckpointKind separates upload and download namespaces. Zero is invalid.
type CheckpointKind string

const (
	CheckpointUpload   CheckpointKind = "upload"
	CheckpointDownload CheckpointKind = "download"
)

// UploadState contains confirmed progress, not credentials or signed URLs.
// Session may contain only non-sensitive provider session identifiers.
// A resumed Writer also needs a replayable source or its complete retained spool.
type UploadState struct {
	Size       int64             `json:"size"`
	SHA256     string            `json:"sha256"`
	PartSize   int64             `json:"part_size"`
	TotalParts int64             `json:"total_parts"`
	Completed  []PartInfo        `json:"completed"`
	Session    map[string]string `json:"session,omitempty"`
}

// Validate rejects inconsistent part geometry and unconfirmed or duplicate progress.
func (s UploadState) Validate() error {
	if s.Size < 0 || s.PartSize <= 0 || !validSHA256(s.SHA256) {
		return invalidCheckpoint("invalid upload size, part size or sha256")
	}
	var total int64
	if s.Size > 0 {
		total = 1 + (s.Size-1)/s.PartSize
	}
	if s.TotalParts != total {
		return invalidCheckpoint("incorrect part count")
	}
	seen := make(map[int]bool, len(s.Completed))
	for _, p := range s.Completed {
		if p.PartNumber < 1 || int64(p.PartNumber) > total || seen[p.PartNumber] {
			return invalidCheckpoint("invalid or duplicate completed part")
		}
		seen[p.PartNumber] = true
		want := min(s.PartSize, s.Size-(int64(p.PartNumber)-1)*s.PartSize)
		if p.Size != want {
			return invalidCheckpoint("incorrect completed part size")
		}
	}
	return nil
}

// MatchSource refuses reuse when the source content has changed.
func (s UploadState) MatchSource(size int64, sha256 string) error {
	if err := s.Validate(); err != nil {
		return err
	}
	if size != s.Size || !strings.EqualFold(sha256, s.SHA256) {
		return invalidCheckpoint("source content changed")
	}
	return nil
}

// DownloadState binds confirmed bytes to both cloud identity and local files.
// Target and Partial must be absolute, clean paths; Partial must be Target+".part".
type DownloadState struct {
	FileID  string    `json:"file_id"`
	Size    int64     `json:"size"`
	ETag    string    `json:"etag,omitempty"`
	MD5     string    `json:"md5,omitempty"`
	SHA1    string    `json:"sha1,omitempty"`
	SHA256  string    `json:"sha256,omitempty"`
	ModTime time.Time `json:"mod_time"`
	Offset  int64     `json:"offset"`
	Target  string    `json:"target"`
	Partial string    `json:"partial"`
}

// Validate requires a version validator, a valid offset and an absolute file binding.
func (s DownloadState) Validate() error {
	if s.FileID == "" || s.Size < 0 || s.Offset < 0 || s.Offset > s.Size {
		return invalidCheckpoint("invalid download identity or offset")
	}
	if (s.MD5 != "" && !validDigest(s.MD5, 16)) ||
		(s.SHA1 != "" && !validDigest(s.SHA1, 20)) ||
		(s.SHA256 != "" && !validSHA256(s.SHA256)) {
		return invalidCheckpoint("invalid download hash")
	}
	if s.ETag == "" && s.MD5 == "" && s.SHA1 == "" && s.SHA256 == "" && s.ModTime.IsZero() {
		return invalidCheckpoint("download has no object version validator")
	}
	if !filepath.IsAbs(s.Target) || filepath.Clean(s.Target) != s.Target || s.Partial != s.Target+".part" {
		return invalidCheckpoint("invalid local download binding")
	}
	return nil
}

// MatchObject checks every recorded version validator and the local destination.
func (s DownloadState) MatchObject(info ObjectInfo, dst string) error {
	if err := s.Validate(); err != nil {
		return err
	}
	target, err := filepath.Abs(dst)
	if err != nil {
		return fmt.Errorf("resolve download destination: %w", err)
	}
	if s.FileID != info.ID || s.Size != info.Size || s.Target != target ||
		(s.ETag != "" && s.ETag != info.ETag) ||
		(s.MD5 != "" && !strings.EqualFold(s.MD5, info.MD5)) ||
		(s.SHA1 != "" && !strings.EqualFold(s.SHA1, info.SHA1)) ||
		(s.SHA256 != "" && !strings.EqualFold(s.SHA256, info.SHA256)) ||
		(!s.ModTime.IsZero() && !s.ModTime.Equal(info.ModTime)) {
		return invalidCheckpoint("download object or destination changed")
	}
	return nil
}

func validSHA256(value string) bool {
	return validDigest(value, sha256.Size)
}

func validDigest(value string, size int) bool {
	data, err := hex.DecodeString(value)
	return err == nil && len(data) == size
}

func invalidCheckpoint(message string) error {
	return fmt.Errorf("%w: %s", ErrInvalidCheckpoint, message)
}

// Validate checks the envelope. DecodeUpload/DecodeDownload validate operation data.
func (c Checkpoint) Validate() error {
	if c.Version != CheckpointVersion {
		return invalidCheckpoint("unsupported format version")
	}
	if c.Kind != CheckpointUpload && c.Kind != CheckpointDownload {
		return invalidCheckpoint("unknown operation kind")
	}
	if c.Provider == "" || c.AccountID == "" {
		return invalidCheckpoint("missing provider or account")
	}
	if !utf8.ValidString(c.Provider) || !utf8.ValidString(c.AccountID) || !utf8.ValidString(c.ResumeID) {
		return invalidCheckpoint("invalid identity encoding")
	}
	if _, err := pathx.Key(c.Path); err != nil {
		return invalidCheckpoint("invalid cloud path")
	}
	data := bytes.TrimSpace(c.Data)
	if len(data) == 0 || data[0] != '{' || !json.Valid(data) {
		return invalidCheckpoint("data must be a json object")
	}
	if c.ID == "" || c.ID != c.fingerprint() {
		return invalidCheckpoint("checkpoint identity mismatch")
	}
	return nil
}

func (c Checkpoint) fingerprint() string {
	// An array gives unambiguous field boundaries and a stable encoding order.
	// A fixed array of strings has no unsupported values or custom marshalers.
	data, _ := json.Marshal([5]string{string(c.Kind), c.Provider, c.AccountID, c.Path, c.ResumeID})
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// DecodeUpload validates both the envelope and confirmed upload progress.
func (c Checkpoint) DecodeUpload() (UploadState, error) {
	var state UploadState
	if err := c.Validate(); err != nil {
		return state, err
	}
	if c.Kind != CheckpointUpload {
		return state, invalidCheckpoint("not an upload checkpoint")
	}
	if err := json.Unmarshal(c.Data, &state); err != nil {
		return state, fmt.Errorf("%w: %w", ErrInvalidCheckpoint, err)
	}
	return state, state.Validate()
}

// DecodeDownload validates both the envelope and the local/object binding.
func (c Checkpoint) DecodeDownload() (DownloadState, error) {
	var state DownloadState
	if err := c.Validate(); err != nil {
		return state, err
	}
	if c.Kind != CheckpointDownload {
		return state, invalidCheckpoint("not a download checkpoint")
	}
	if err := json.Unmarshal(c.Data, &state); err != nil {
		return state, fmt.Errorf("%w: %w", ErrInvalidCheckpoint, err)
	}
	return state, state.Validate()
}

// newCheckpoint is internal: callers supply logical keys, never cloud paths.
func (c *Client) newCheckpoint(ctx context.Context, key string, kind CheckpointKind, resumeID string, data json.RawMessage) (Checkpoint, error) {
	if err := ctx.Err(); err != nil {
		return Checkpoint{}, err
	}
	_, full, err := c.resolve(key)
	if err != nil {
		return Checkpoint{}, err
	}
	account, err := c.provider.AccountID(ctx)
	if err != nil {
		return Checkpoint{}, c.wrapError("checkpoint identity", key, err)
	}
	cp := Checkpoint{Version: CheckpointVersion, Kind: kind, Provider: c.provider.Name(), AccountID: account,
		Path: full, ResumeID: resumeID, Data: append(json.RawMessage(nil), data...), UpdatedAt: time.Now().UTC()}
	cp.ID = cp.fingerprint()
	return cp, cp.Validate()
}

func (c *Client) checkResume(op, key string, enabled bool, resumeID string, writer bool) error {
	if !enabled {
		if resumeID == "" {
			return nil
		}
		return &Error{Op: op, Provider: c.provider.Name(), Key: key, Kind: ErrInvalidArgument, Err: fmt.Errorf("resume id requires resume")}
	}
	if c.config.CheckpointStore == nil || (writer && strings.TrimSpace(resumeID) == "") {
		return &Error{Op: op, Provider: c.provider.Name(), Key: key, Kind: ErrInvalidArgument, Err: fmt.Errorf("resume requires a checkpoint store and writers require a stable resume id")}
	}
	// Phase 2/3 will attach provider recovery to the common state models.
	return &Error{Op: op, Provider: c.provider.Name(), Key: key, Kind: ErrUnsupported, Err: fmt.Errorf("resume is not implemented in phase 1")}
}
