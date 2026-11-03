package cloudstorage

import (
	"context"
	"io"
	"time"
)

const (
	DefaultPartSize           int64 = 20 << 20
	DefaultMultipartThreshold int64 = 20 << 20
	DefaultUploadConcurrency        = 1
	DefaultCleanupTimeout           = 5 * time.Second
)

// ObjectInfo describes a cloud object. Key is always relative to Client Prefix.
type ObjectInfo struct {
	Key      string
	ID       string
	Size     int64
	ETag     string
	MD5      string
	SHA1     string
	SHA256   string
	ModTime  time.Time
	Metadata map[string]string
}

// PartInfo identifies one uploaded multipart segment.
type PartInfo struct {
	PartNumber int    `json:"part_number"`
	Size       int64  `json:"size"`
	ETag       string `json:"etag"`
}

// Progress reports bytes transferred by an operation. Total is -1 when unknown.
type Progress struct {
	Transferred int64
	Total       int64
}

type ProgressFunc func(Progress)

// UploadOptions controls a single upload. Nil Size means the size is unknown.
type UploadOptions struct {
	// Resume requires a CheckpointStore and provider recovery support.
	Resume             bool
	ResumeID           string
	Size               *int64
	PartSize           int64
	MultipartThreshold int64
	Concurrency        int
	DisableMultipart   bool
	TempDir            string
	MD5                string
	SHA1               string
	SHA256             string
	Progress           ProgressFunc
}

// WriterOptions controls streaming upload behavior.
type WriterOptions struct {
	// Resume requires a CheckpointStore and a stable, non-empty ResumeID.
	// It also requires a replayable source or a retained complete spool file.
	// Providers without recovery support return ErrUnsupported before transfer I/O.
	Resume             bool
	ResumeID           string
	Size               *int64
	PartSize           int64
	MultipartThreshold int64
	Concurrency        int
	DisableMultipart   bool
	TempDir            string
	MD5                string
	SHA1               string
	SHA256             string
	Progress           ProgressFunc
}

// ReadOptions requests a byte range. Length zero means reading to EOF.
type ReadOptions struct {
	Offset int64
	Length int64
}

// DownloadOptions controls downloading into a local file.
type DownloadOptions struct {
	// Resume requires a CheckpointStore and provider Range validation support.
	Resume bool
	// KeepPartial retains .part on failure. It alone does not enable safe resume.
	KeepPartial bool
	Progress    ProgressFunc
}

// Capabilities describes protocol constraints that affect generic upload orchestration.
type Capabilities struct {
	SupportsStreamingMultipart bool
	RequiresFullHash           bool
	RequiresMultipart          bool
	SupportsDownloadResume     bool
}

// ResumableUploader owns provider session recovery and persistent confirmed parts.
// It must not abort a recoverable session on a transient failure. Source is complete,
// seekable and hashed; checkpoint contains the common source identity and namespace.
type ResumableUploader interface {
	UploadResumable(ctx context.Context, path string, source io.ReadSeeker, opts UploadOptions, checkpoint Checkpoint, store CheckpointStore) (ObjectInfo, error)
	AbortUpload(ctx context.Context, path string, checkpoint Checkpoint, store CheckpointStore) error
}

// PendingUpload exposes a complete retained spool after a resumable Close failure.
// Re-upload this file with the same key and ResumeID to continue confirmed parts.
type PendingUpload interface {
	UploadWriter
	ResumeSource() string
}

// Provider is the cloud-specific SPI. Paths passed to it include the normalized Prefix.
// Every method must honor ctx, including cancellation during remote cleanup.
// HTTP clients and TokenStores belong in provider constructors, not Client Config.
type Provider interface {
	Name() string
	AccountID(ctx context.Context) (string, error)
	Capabilities() Capabilities
	Stat(ctx context.Context, path string) (ObjectInfo, error)
	Open(ctx context.Context, path string, opts ReadOptions) (io.ReadCloser, error)
	Upload(ctx context.Context, path string, r io.Reader, opts UploadOptions) (ObjectInfo, error)
	CreateMultipart(ctx context.Context, path string, opts UploadOptions) (MultipartSession, error)
	Delete(ctx context.Context, path string) error
}

// MultipartSession is an active provider-specific upload session.
type MultipartSession interface {
	UploadPart(ctx context.Context, partNumber int, r io.Reader, size int64) (PartInfo, error)
	Complete(ctx context.Context, parts []PartInfo) (ObjectInfo, error)
	Abort(ctx context.Context) error
}

// MultipartGeometry lets a provider enforce the part size returned by its server.
type MultipartGeometry interface{ PartSize() int64 }

// UploadWriter extends io.WriteCloser with abort and result inspection.
// Abort abandons the upload and removes local temporary resources; it is not pause.
// Methods must be called serially. Close errors are sticky; Abort may retry cleanup.
type UploadWriter interface {
	io.WriteCloser
	Abort(ctx context.Context) error
	Info() ObjectInfo
}

// Storage is the public cloud storage contract.
type Storage interface {
	Stat(ctx context.Context, key string) (ObjectInfo, error)
	Upload(ctx context.Context, key string, r io.Reader, opts UploadOptions) (ObjectInfo, error)
	UploadFile(ctx context.Context, key, src string, opts UploadOptions) (ObjectInfo, error)
	Open(ctx context.Context, key string, opts ReadOptions) (io.ReadCloser, error)
	DownloadToFile(ctx context.Context, key, dst string, opts DownloadOptions) error
	NewWriter(ctx context.Context, key string, opts WriterOptions) (UploadWriter, error)
	Delete(ctx context.Context, key string) error
}

// Config contains provider-independent client settings.
type Config struct {
	Prefix             string
	PartSize           int64
	MultipartThreshold int64
	Concurrency        int
	TempDir            string
	CheckpointStore    CheckpointStore
	// CleanupTimeout bounds remote cleanup, including after cancellation.
	// Zero selects DefaultCleanupTimeout; negative values are invalid.
	CleanupTimeout time.Duration
}
