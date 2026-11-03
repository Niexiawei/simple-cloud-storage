package cloudstorage

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
)

const maxInlineUploadSize int64 = 8 << 20

type writerMode uint8

const (
	writerSmall writerMode = iota
	writerMultipart
	writerSpool
)

type uploadWriter struct {
	ctx      context.Context
	client   *Client
	key      string
	full     string
	opts     UploadOptions
	mode     writerMode
	expected *int64
	received int64
	uploaded int64

	small   []byte
	partBuf []byte
	session MultipartSession
	parts   []PartInfo

	file          *os.File
	filePath      string
	spool         io.Writer
	spoolComplete bool
	md5           hash.Hash
	sha1          hash.Hash
	sha256        hash.Hash

	info     ObjectInfo
	err      error
	closeErr error
	closed   bool
	aborted  bool
}

func (c *Client) NewWriter(ctx context.Context, key string, options WriterOptions) (UploadWriter, error) {
	return c.newWriter(ctx, key, options, true)
}

func (c *Client) newWriter(ctx context.Context, key string, options WriterOptions, requireID bool) (UploadWriter, error) {
	if err := c.checkResume("new writer", key, options.Resume, options.ResumeID, requireID); err != nil {
		return nil, err
	}
	logical, full, err := c.resolve(key)
	if err != nil {
		return nil, err
	}
	opts := UploadOptions{
		Resume:             options.Resume,
		ResumeID:           options.ResumeID,
		Size:               options.Size,
		PartSize:           options.PartSize,
		MultipartThreshold: options.MultipartThreshold,
		Concurrency:        options.Concurrency,
		DisableMultipart:   options.DisableMultipart,
		TempDir:            options.TempDir,
		MaxTempFileSize:    options.MaxTempFileSize,
		MD5:                options.MD5,
		SHA1:               options.SHA1,
		SHA256:             options.SHA256,
		Progress:           options.Progress,
	}
	if opts.PartSize == 0 {
		opts.PartSize = c.config.PartSize
	}
	if opts.MultipartThreshold == 0 {
		opts.MultipartThreshold = c.config.MultipartThreshold
	}
	if opts.Concurrency == 0 {
		opts.Concurrency = c.config.Concurrency
	}
	if opts.TempDir == "" {
		opts.TempDir = c.config.TempDir
	}
	if opts.MaxTempFileSize == 0 {
		opts.MaxTempFileSize = c.config.MaxTempFileSize
	}
	if opts.MaxTempFileSize < 0 || opts.PartSize < 1 || opts.PartSize > MaxPartSize || opts.MultipartThreshold < 1 || int64(int(opts.PartSize)) != opts.PartSize {
		return nil, &Error{Op: "new writer", Provider: c.provider.Name(), Key: logical, Kind: ErrInvalidArgument, Err: fmt.Errorf("invalid part size, multipart threshold or temporary file limit")}
	}
	if opts.Concurrency != 1 {
		return nil, &Error{Op: "new writer", Provider: c.provider.Name(), Key: logical, Kind: ErrUnsupported, Err: fmt.Errorf("only concurrency=1 is supported")}
	}
	if opts.Size != nil && *opts.Size < 0 {
		return nil, &Error{Op: "new writer", Provider: c.provider.Name(), Key: logical, Kind: ErrInvalidArgument, Err: fmt.Errorf("size is negative")}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	w := &uploadWriter{ctx: ctx, client: c, key: logical, full: full, opts: opts}
	if opts.Size != nil {
		size := *opts.Size
		w.expected = &size
		opts.Size = &size
		w.opts.Size = &size
	}
	caps := c.provider.Capabilities()
	if caps.RequiresMultipart && opts.DisableMultipart {
		return nil, ErrUnsupported
	}
	switch {
	case opts.Resume || opts.Size == nil || caps.RequiresFullHash || !caps.SupportsStreamingMultipart:
		w.mode = writerSpool
		if err := w.initSpool(); err != nil {
			return nil, c.wrapError("new writer", logical, err)
		}
	case !opts.DisableMultipart && *opts.Size >= opts.MultipartThreshold:
		w.mode = writerMultipart
		session, err := c.provider.CreateMultipart(ctx, full, opts)
		if err != nil {
			return nil, c.wrapError("new writer", logical, err)
		}
		w.session = session
		if geometry, ok := session.(MultipartGeometry); ok {
			partSize := geometry.PartSize()
			if partSize <= 0 || partSize > MaxPartSize || int64(int(partSize)) != partSize {
				return nil, w.failClose(ErrProtocol)
			}
			opts.PartSize = partSize
		}
		w.partBuf = make([]byte, 0, int(min(opts.PartSize, *opts.Size)))
	case *opts.Size <= maxInlineUploadSize:
		w.mode = writerSmall
		w.small = make([]byte, 0, int(*opts.Size))
	default:
		w.mode = writerSpool
		if err := w.initSpool(); err != nil {
			return nil, c.wrapError("new writer", logical, err)
		}
	}
	return w, nil
}

func (w *uploadWriter) Write(p []byte) (int, error) {
	if w.aborted {
		return 0, ErrWriterAborted
	}
	if w.closed {
		return 0, ErrWriterClosed
	}
	if w.err != nil {
		return 0, w.err
	}
	if err := w.ctx.Err(); err != nil {
		w.err = err
		return 0, err
	}
	if w.expected != nil && int64(len(p)) > *w.expected-w.received {
		w.err = fmt.Errorf("%w: declared=%d received would be %d", ErrSizeMismatch, *w.expected, w.received+int64(len(p)))
		return 0, w.err
	}
	if len(p) == 0 {
		return 0, nil
	}
	if w.mode == writerSpool && w.opts.MaxTempFileSize > 0 && int64(len(p)) > w.opts.MaxTempFileSize-w.received {
		w.err = ErrTempLimit
		return 0, w.err
	}

	var n int
	var err error
	switch w.mode {
	case writerSpool:
		n, err = w.spool.Write(p)
		if n < len(p) && err == nil {
			err = io.ErrShortWrite
		}
	case writerSmall:
		w.small = append(w.small, p...)
		n = len(p)
	case writerMultipart:
		n, err = w.writeMultipart(p)
	default:
		err = fmt.Errorf("unknown writer mode")
	}
	w.received += int64(n)
	if err != nil {
		w.err = err
	}
	return n, err
}

func (w *uploadWriter) Close() error {
	if w.closeErr != nil {
		return w.closeErr
	}
	if w.aborted {
		return ErrWriterAborted
	}
	if w.closed {
		return nil
	}
	defer w.releaseTempRegistration()
	w.closed = true
	if w.err != nil {
		return w.failClose(w.err)
	}
	if err := w.ctx.Err(); err != nil {
		return w.failClose(err)
	}
	if w.expected != nil && w.received != *w.expected {
		return w.failClose(fmt.Errorf("%w: declared=%d received=%d", ErrSizeMismatch, *w.expected, w.received))
	}

	var info ObjectInfo
	var err error
	switch w.mode {
	case writerSmall:
		w.setHashes(w.small)
		info, err = w.uploadPrepared(bytes.NewReader(w.small))
	case writerMultipart:
		if err = w.flushPart(); err == nil {
			if err = w.ctx.Err(); err == nil {
				info, err = w.session.Complete(w.ctx, w.parts)
			}
		}
		if err == nil {
			w.session = nil
		}
	case writerSpool:
		info, err = w.closeSpool()
	}
	if err != nil {
		return w.failClose(err)
	}
	info.Key = w.key
	w.info = info
	return nil
}

func (w *uploadWriter) failClose(err error) error {
	if w.opts.Resume && w.spoolComplete {
		w.closeErr = err
		return err
	}
	ctx, cancel := w.client.cleanupContext(w.ctx)
	defer cancel()
	w.closeErr = errors.Join(err, w.abortResources(ctx))
	return w.closeErr
}

func (w *uploadWriter) Abort(ctx context.Context) error {
	if w.closed && w.closeErr == nil && !w.aborted {
		return nil
	}
	w.aborted = true
	w.closed = true
	cleanupCtx, cancel := context.WithTimeout(ctx, w.client.config.CleanupTimeout)
	defer cancel()
	var remoteErr error
	if w.opts.Resume && w.spoolComplete {
		checkpoint, err := w.client.newCheckpoint(cleanupCtx, w.key, CheckpointUpload, w.opts.ResumeID, json.RawMessage(`{}`))
		if err != nil {
			remoteErr = err
		} else if provider, ok := w.client.provider.(ResumableUploader); ok {
			remoteErr = provider.AbortUpload(cleanupCtx, w.full, checkpoint, w.client.config.CheckpointStore)
		}
	}
	return errors.Join(remoteErr, w.abortResources(cleanupCtx))
}

func (w *uploadWriter) Info() ObjectInfo {
	return w.info
}

func (w *uploadWriter) ResumeSource() string {
	if w.opts.Resume && w.spoolComplete {
		return w.filePath
	}
	return ""
}

func (w *uploadWriter) writeMultipart(p []byte) (int, error) {
	written := 0
	for len(p) > 0 {
		n := copy(w.partBuf[len(w.partBuf):cap(w.partBuf)], p)
		w.partBuf = w.partBuf[:len(w.partBuf)+n]
		p = p[n:]
		written += n
		if len(w.partBuf) == cap(w.partBuf) {
			if err := w.flushPart(); err != nil {
				return written, err
			}
		}
	}
	return written, nil
}

func (w *uploadWriter) flushPart() error {
	if err := w.ctx.Err(); err != nil {
		return err
	}
	if len(w.partBuf) == 0 {
		return nil
	}
	partNumber := len(w.parts) + 1
	size := int64(len(w.partBuf))
	part, err := w.session.UploadPart(w.ctx, partNumber, bytes.NewReader(w.partBuf), size)
	if err != nil {
		return err
	}
	part.PartNumber = partNumber
	part.Size = size
	w.parts = append(w.parts, part)
	w.uploaded += size
	if w.opts.Progress != nil {
		w.opts.Progress(Progress{Transferred: w.uploaded, Total: *w.expected})
	}
	w.partBuf = w.partBuf[:0]
	return nil
}

func (w *uploadWriter) initSpool() error {
	if w.opts.MaxTempFileSize > 0 && w.expected != nil && *w.expected > w.opts.MaxTempFileSize {
		return ErrTempLimit
	}
	f, err := os.CreateTemp(w.opts.TempDir, ".simple-cloud-storage-*.upload")
	if err != nil {
		return err
	}
	w.file = f
	w.filePath = f.Name()
	activeTempUploads.Store(tempPathKey(w.filePath), true)
	w.md5 = md5.New()
	w.sha1 = sha1.New()
	w.sha256 = sha256.New()
	w.spool = io.MultiWriter(f, w.md5, w.sha1, w.sha256)
	return nil
}

func (w *uploadWriter) closeSpool() (ObjectInfo, error) {
	if err := w.file.Sync(); err != nil {
		return ObjectInfo{}, err
	}
	if err := w.file.Close(); err != nil {
		return ObjectInfo{}, err
	}
	w.file = nil
	w.spoolComplete = true
	f, err := os.Open(w.filePath)
	if err != nil {
		return ObjectInfo{}, err
	}
	w.opts.MD5 = hex.EncodeToString(w.md5.Sum(nil))
	w.opts.SHA1 = hex.EncodeToString(w.sha1.Sum(nil))
	w.opts.SHA256 = hex.EncodeToString(w.sha256.Sum(nil))
	size := w.received
	w.opts.Size = &size
	info, uploadErr := w.uploadPrepared(f)
	closeErr := f.Close()
	if uploadErr != nil && w.opts.Resume {
		return info, errors.Join(uploadErr, closeErr)
	}
	removeErr := os.Remove(w.filePath)
	if removeErr == nil || os.IsNotExist(removeErr) {
		w.releaseTempRegistration()
		w.filePath = ""
		removeErr = nil
	}
	return info, errors.Join(uploadErr, closeErr, removeErr)
}

func (w *uploadWriter) setHashes(data []byte) {
	md5Sum := md5.Sum(data)
	sha1Sum := sha1.Sum(data)
	sha256Sum := sha256.Sum256(data)
	w.opts.MD5 = hex.EncodeToString(md5Sum[:])
	w.opts.SHA1 = hex.EncodeToString(sha1Sum[:])
	w.opts.SHA256 = hex.EncodeToString(sha256Sum[:])
}

func (w *uploadWriter) abortResources(ctx context.Context) error {
	var errs []error
	if w.session != nil {
		if err := w.session.Abort(ctx); err != nil {
			errs = append(errs, err)
		} else {
			w.session = nil
		}
	}
	if w.file != nil {
		if err := w.file.Close(); err != nil {
			errs = append(errs, err)
		} else {
			w.file = nil
		}
	}
	if w.filePath != "" {
		if err := os.Remove(w.filePath); err != nil && !os.IsNotExist(err) {
			errs = append(errs, err)
		} else {
			w.releaseTempRegistration()
			w.filePath = ""
		}
	}
	return errors.Join(errs...)
}

func (w *uploadWriter) releaseTempRegistration() {
	if w.filePath != "" {
		activeTempUploads.Delete(tempPathKey(w.filePath))
	}
}
