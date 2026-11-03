package cloudstorage

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/Niexiawei/simple-cloud-storage/internal/pathx"
)

// Client provides provider-independent storage operations.
type Client struct {
	provider Provider
	prefix   string
	config   Config
}

var _ Storage = (*Client)(nil)

func NewClient(provider Provider, cfg Config) (*Client, error) {
	if provider == nil {
		return nil, &Error{Op: "new client", Kind: ErrInvalidArgument, Err: fmt.Errorf("provider is nil")}
	}
	prefix, err := pathx.Prefix(cfg.Prefix)
	if err != nil {
		return nil, &Error{Op: "new client", Provider: provider.Name(), Kind: ErrInvalidArgument, Err: err}
	}
	if cfg.PartSize == 0 {
		cfg.PartSize = DefaultPartSize
	}
	if cfg.MultipartThreshold == 0 {
		cfg.MultipartThreshold = DefaultMultipartThreshold
	}
	if cfg.Concurrency == 0 {
		cfg.Concurrency = DefaultUploadConcurrency
	}
	if cfg.CleanupTimeout < 0 {
		return nil, &Error{Op: "new client", Provider: provider.Name(), Kind: ErrInvalidArgument, Err: fmt.Errorf("cleanup timeout is negative")}
	}
	if cfg.CleanupTimeout == 0 {
		cfg.CleanupTimeout = DefaultCleanupTimeout
	}
	if cfg.PartSize < 1 || cfg.MultipartThreshold < 1 {
		return nil, &Error{Op: "new client", Provider: provider.Name(), Kind: ErrInvalidArgument, Err: fmt.Errorf("part size and multipart threshold must be positive")}
	}
	if cfg.Concurrency != 1 {
		return nil, &Error{Op: "new client", Provider: provider.Name(), Kind: ErrUnsupported, Err: fmt.Errorf("only concurrency=1 is supported")}
	}
	if int64(int(cfg.PartSize)) != cfg.PartSize {
		return nil, &Error{Op: "new client", Provider: provider.Name(), Kind: ErrInvalidArgument, Err: fmt.Errorf("part size exceeds platform int capacity")}
	}
	cfg.Prefix = prefix
	return &Client{provider: provider, prefix: prefix, config: cfg}, nil
}

func (c *Client) Prefix() string {
	return c.prefix
}

func (c *Client) Stat(ctx context.Context, key string) (ObjectInfo, error) {
	logical, full, err := c.resolve(key)
	if err != nil {
		return ObjectInfo{}, err
	}
	info, err := c.provider.Stat(ctx, full)
	if err != nil {
		return ObjectInfo{}, c.wrapError("stat", logical, err)
	}
	info.Key = logical
	return info, nil
}

func (c *Client) Open(ctx context.Context, key string, opts ReadOptions) (io.ReadCloser, error) {
	logical, full, err := c.resolve(key)
	if err != nil {
		return nil, err
	}
	if opts.Offset < 0 || opts.Length < 0 {
		return nil, &Error{Op: "open", Provider: c.provider.Name(), Key: logical, Kind: ErrInvalidArgument, Err: fmt.Errorf("negative range")}
	}
	r, err := c.provider.Open(ctx, full, opts)
	if err != nil {
		return nil, c.wrapError("open", logical, err)
	}
	return r, nil
}

func (c *Client) Delete(ctx context.Context, key string) error {
	logical, full, err := c.resolve(key)
	if err != nil {
		return err
	}
	if err := c.provider.Delete(ctx, full); err != nil {
		return c.wrapError("delete", logical, err)
	}
	return nil
}

func (c *Client) Upload(ctx context.Context, key string, r io.Reader, opts UploadOptions) (ObjectInfo, error) {
	if err := c.checkResume("upload", key, opts.Resume, opts.ResumeID, false); err != nil {
		return ObjectInfo{}, err
	}
	if r == nil {
		return ObjectInfo{}, &Error{Op: "upload", Provider: c.provider.Name(), Key: key, Kind: ErrInvalidArgument, Err: fmt.Errorf("reader is nil")}
	}
	w, err := c.newWriter(ctx, key, WriterOptions{
		Resume:             opts.Resume,
		ResumeID:           opts.ResumeID,
		Size:               opts.Size,
		PartSize:           opts.PartSize,
		MultipartThreshold: opts.MultipartThreshold,
		Concurrency:        opts.Concurrency,
		DisableMultipart:   opts.DisableMultipart,
		TempDir:            opts.TempDir,
		MD5:                opts.MD5,
		SHA1:               opts.SHA1,
		SHA256:             opts.SHA256,
		Progress:           opts.Progress,
	}, false)
	if err != nil {
		return ObjectInfo{}, err
	}
	if _, err := io.Copy(w, &contextReader{ctx: ctx, r: r}); err != nil {
		cleanupCtx, cancel := c.cleanupContext(ctx)
		abortErr := w.Abort(cleanupCtx)
		cancel()
		return ObjectInfo{}, c.wrapError("upload", key, errors.Join(err, abortErr))
	}
	if err := w.Close(); err != nil {
		return ObjectInfo{}, c.wrapError("upload", key, err)
	}
	return w.Info(), nil
}

func (c *Client) UploadFile(ctx context.Context, key, src string, opts UploadOptions) (ObjectInfo, error) {
	if err := c.checkResume("upload file", key, opts.Resume, opts.ResumeID, false); err != nil {
		return ObjectInfo{}, err
	}
	f, err := os.Open(src)
	if err != nil {
		return ObjectInfo{}, &Error{Op: "upload file", Provider: c.provider.Name(), Key: key, Err: err}
	}
	defer f.Close()
	stat, err := f.Stat()
	if err != nil {
		return ObjectInfo{}, &Error{Op: "upload file", Provider: c.provider.Name(), Key: key, Err: err}
	}
	if !stat.Mode().IsRegular() {
		return ObjectInfo{}, &Error{Op: "upload file", Provider: c.provider.Name(), Key: key, Kind: ErrInvalidArgument, Err: fmt.Errorf("source is not a regular file")}
	}
	size := stat.Size()
	if opts.Size != nil && *opts.Size != size {
		return ObjectInfo{}, &Error{Op: "upload file", Provider: c.provider.Name(), Key: key, Kind: ErrSizeMismatch, Err: fmt.Errorf("declared=%d actual=%d", *opts.Size, size)}
	}
	opts.Size = &size
	return c.Upload(ctx, key, f, opts)
}

func (c *Client) DownloadToFile(ctx context.Context, key, dst string, opts DownloadOptions) error {
	if err := c.checkResume("download", key, opts.Resume, "", false); err != nil {
		return err
	}
	if opts.Resume {
		return c.downloadResumable(ctx, key, dst, opts)
	}
	if dst == "" {
		return &Error{Op: "download", Provider: c.provider.Name(), Key: key, Kind: ErrInvalidArgument, Err: fmt.Errorf("destination is empty")}
	}
	var expected *ObjectInfo
	if c.provider.Capabilities().SupportsDownloadResume {
		info, err := c.Stat(ctx, key)
		if err != nil {
			return err
		}
		expected = &info
	}
	r, err := c.Open(ctx, key, ReadOptions{})
	if err != nil {
		return err
	}
	defer r.Close()

	part := dst + ".part"
	f, err := os.OpenFile(part, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return &Error{Op: "download", Provider: c.provider.Name(), Key: key, Err: err}
	}
	completed := false
	defer func() {
		_ = f.Close()
		if !completed && !opts.KeepPartial {
			_ = os.Remove(part)
		}
	}()

	writer := io.Writer(f)
	if opts.Progress != nil {
		writer = &progressWriter{ctx: ctx, w: f, total: -1, progress: opts.Progress}
	}
	if _, err := io.Copy(writer, &contextReader{ctx: ctx, r: r}); err != nil {
		return &Error{Op: "download", Provider: c.provider.Name(), Key: key, Err: err}
	}
	if err := f.Sync(); err != nil {
		return &Error{Op: "download", Provider: c.provider.Name(), Key: key, Err: err}
	}
	if err := f.Close(); err != nil {
		return &Error{Op: "download", Provider: c.provider.Name(), Key: key, Err: err}
	}
	if expected != nil {
		local, err := os.Open(part)
		if err != nil {
			return err
		}
		verifyErr := verifyLocalHashes(ctx, local, *expected)
		closeErr := local.Close()
		if err := errors.Join(verifyErr, closeErr); err != nil {
			return err
		}
	}
	if err := replaceLocalFile(part, dst); err != nil {
		return &Error{Op: "download", Provider: c.provider.Name(), Key: key, Err: err}
	}
	completed = true
	return nil
}

func (c *Client) resolve(key string) (string, string, error) {
	logical, err := pathx.Key(key)
	if err != nil {
		return "", "", &Error{Op: "resolve", Provider: c.provider.Name(), Key: key, Kind: ErrInvalidArgument, Err: err}
	}
	return logical, c.prefix + "/" + logical, nil
}

func (c *Client) cleanupContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), c.config.CleanupTimeout)
}

func (c *Client) wrapError(op, key string, err error) error {
	if err == nil {
		return nil
	}
	var storageErr *Error
	if errors.As(err, &storageErr) {
		return err
	}
	return &Error{Op: op, Provider: c.provider.Name(), Key: key, Err: err}
}

func (w *uploadWriter) uploadPrepared(r io.Reader) (ObjectInfo, error) {
	c, ctx, key, full, opts := w.client, w.ctx, w.key, w.full, w.opts
	if err := ctx.Err(); err != nil {
		return ObjectInfo{}, err
	}
	if opts.Size == nil || *opts.Size < 0 {
		return ObjectInfo{}, &Error{Op: "upload", Provider: c.provider.Name(), Key: key, Kind: ErrInvalidArgument, Err: fmt.Errorf("prepared upload requires a known non-negative size")}
	}
	if opts.Resume {
		source, ok := r.(io.ReadSeeker)
		provider, supported := c.provider.(ResumableUploader)
		if !ok || !supported {
			return ObjectInfo{}, ErrUnsupported
		}
		state := UploadState{Size: *opts.Size, SHA256: opts.SHA256, PartSize: opts.PartSize, Source: w.filePath}
		if state.Size > 0 {
			state.TotalParts = 1 + (state.Size-1)/state.PartSize
		}
		data, err := json.Marshal(state)
		if err != nil {
			return ObjectInfo{}, err
		}
		checkpoint, err := c.newCheckpoint(ctx, key, CheckpointUpload, opts.ResumeID, data)
		if err != nil {
			return ObjectInfo{}, err
		}
		info, err := provider.UploadResumable(ctx, full, source, opts, checkpoint, c.config.CheckpointStore)
		info.Key = key
		return info, err
	}
	if !c.provider.Capabilities().RequiresMultipart && (opts.DisableMultipart || *opts.Size < opts.MultipartThreshold) {
		reader := io.Reader(r)
		if opts.Progress != nil {
			reader = &progressReader{ctx: ctx, r: r, total: *opts.Size, progress: opts.Progress}
		}
		info, err := c.provider.Upload(ctx, full, reader, opts)
		if err != nil {
			return ObjectInfo{}, err
		}
		info.Key = key
		return info, nil
	}

	session, err := c.provider.CreateMultipart(ctx, full, opts)
	if err != nil {
		return ObjectInfo{}, err
	}
	w.session = session
	if geometry, ok := session.(MultipartGeometry); ok {
		opts.PartSize = geometry.PartSize()
		if opts.PartSize <= 0 || int64(int(opts.PartSize)) != opts.PartSize {
			return ObjectInfo{}, ErrProtocol
		}
	}
	parts, err := uploadParts(ctx, session, r, *opts.Size, opts.PartSize, opts.Progress)
	if err != nil {
		return ObjectInfo{}, err
	}
	info, err := session.Complete(ctx, parts)
	if err != nil {
		return ObjectInfo{}, err
	}
	w.session = nil
	info.Key = key
	return info, nil
}

func uploadParts(ctx context.Context, session MultipartSession, r io.Reader, size, partSize int64, progress ProgressFunc) ([]PartInfo, error) {
	buffer := make([]byte, int(partSize))
	remaining := size
	parts := make([]PartInfo, 0)
	var uploaded int64
	for partNumber := 1; remaining > 0; partNumber++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		want := min(partSize, remaining)
		n, err := io.ReadFull(r, buffer[:int(want)])
		if err != nil {
			return nil, fmt.Errorf("%w: part %d read %d of %d bytes: %w", ErrSizeMismatch, partNumber, n, want, err)
		}
		part, err := session.UploadPart(ctx, partNumber, bytes.NewReader(buffer[:n]), want)
		if err != nil {
			return nil, err
		}
		part.PartNumber = partNumber
		part.Size = want
		parts = append(parts, part)
		remaining -= want
		uploaded += want
		if progress != nil {
			progress(Progress{Transferred: uploaded, Total: size})
		}
	}
	return parts, nil
}

type contextReader struct {
	ctx context.Context
	r   io.Reader
}

func (r *contextReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.r.Read(p)
}

type progressReader struct {
	ctx      context.Context
	r        io.Reader
	read     int64
	total    int64
	progress ProgressFunc
}

func (r *progressReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	n, err := r.r.Read(p)
	r.read += int64(n)
	if n > 0 {
		r.progress(Progress{Transferred: r.read, Total: r.total})
	}
	return n, err
}

type progressWriter struct {
	ctx      context.Context
	w        io.Writer
	written  int64
	total    int64
	progress ProgressFunc
}

func (w *progressWriter) Write(p []byte) (int, error) {
	if err := w.ctx.Err(); err != nil {
		return 0, err
	}
	n, err := w.w.Write(p)
	w.written += int64(n)
	if n > 0 {
		w.progress(Progress{Transferred: w.written, Total: w.total})
	}
	return n, err
}

func replaceLocalFile(src, dst string) error {
	return os.Rename(src, dst)
}
