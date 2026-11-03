package quark

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	cloudstorage "github.com/Niexiawei/simple-cloud-storage"
)

func (p *Provider) Open(ctx context.Context, full string, opts cloudstorage.ReadOptions) (io.ReadCloser, error) {
	f, err := p.resolveFile(ctx, full)
	if err != nil {
		return nil, err
	}
	r, err := p.openFile(ctx, f, opts)
	if errors.Is(err, cloudstorage.ErrAuthentication) || errors.Is(err, cloudstorage.ErrPermission) {
		if err := p.RefreshCookies(ctx); err != nil {
			return nil, err
		}
		return p.openFile(ctx, f, opts)
	}
	return r, err
}

func (p *Provider) openFile(ctx context.Context, f file, opts cloudstorage.ReadOptions) (io.ReadCloser, error) {
	if opts.Offset < 0 || opts.Length < 0 || opts.Offset > f.Size {
		return nil, cloudstorage.ErrInvalidArgument
	}
	if opts.Offset == f.Size && f.Size > 0 {
		return io.NopCloser(strings.NewReader("")), nil
	}
	snapshot := p.cookies.snapshot()
	var links []struct {
		ID  string `json:"fid"`
		URL string `json:"download_url"`
	}
	if err := p.request(ctx, http.MethodPost, p.api+"/file/download", nil, map[string]any{"fids": []string{f.ID}}, &links, snapshot, false); err != nil {
		return nil, err
	}
	if len(links) != 1 || links[0].URL == "" || (links[0].ID != "" && links[0].ID != f.ID) {
		return nil, fmt.Errorf("quark download link: missing or inconsistent link data: %w", cloudstorage.ErrProtocol)
	}
	u, err := url.Parse(links[0].URL)
	if err != nil || !validEndpoint(links[0].URL) || !p.downloadHost(u) {
		return nil, fmt.Errorf("quark download link: invalid or untrusted endpoint: %w", cloudstorage.ErrProtocol)
	}
	readCtx, cancel := context.WithCancel(ctx)
	req, err := http.NewRequestWithContext(readCtx, http.MethodGet, u.String(), nil)
	if err != nil {
		cancel()
		return nil, cloudstorage.ErrProtocol
	}
	req.Header.Set("Cookie", snapshot)
	req.Header.Set("User-Agent", userAgent)
	req.Header.Set("Referer", "https://pan.quark.cn/")
	req.Header.Set("Accept-Encoding", "identity")
	want := f.Size - opts.Offset
	if opts.Length > 0 {
		want = min(want, opts.Length)
	}
	ranged := want > 0 && (opts.Offset > 0 || opts.Length > 0)
	if ranged {
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", opts.Offset, opts.Offset+want-1))
	}
	client := *p.transfer
	client.CheckRedirect = func(next *http.Request, via []*http.Request) error {
		if len(via) >= 10 || !validEndpoint(next.URL.String()) || !p.downloadHost(next.URL) {
			return cloudstorage.ErrProtocol
		}
		next.Header.Set("Cookie", snapshot)
		return nil
	}
	resp, err := client.Do(req)
	if err != nil {
		cancel()
		return nil, safeTransport(ctx, err)
	}
	fail := func(err error) (io.ReadCloser, error) { resp.Body.Close(); cancel(); return nil, err }
	if ranged {
		if resp.StatusCode >= 400 {
			return fail(statusError(resp.StatusCode, nil, resp.Header.Get("Retry-After")))
		}
		if resp.StatusCode != http.StatusPartialContent {
			return fail(responseProtocolError(resp.StatusCode, "range response must be HTTP 206"))
		}
		expected := fmt.Sprintf("bytes %d-%d/%d", opts.Offset, opts.Offset+want-1, f.Size)
		if resp.Header.Get("Content-Range") != expected {
			return fail(responseProtocolError(resp.StatusCode, "range response does not match requested interval and object size"))
		}
	} else if resp.StatusCode != http.StatusOK {
		return fail(statusError(resp.StatusCode, nil, resp.Header.Get("Retry-After")))
	}
	if resp.ContentLength >= 0 && resp.ContentLength != want {
		return fail(cloudstorage.ErrSizeMismatch)
	}
	if encoding := resp.Header.Get("Content-Encoding"); encoding != "" && encoding != "identity" {
		return fail(cloudstorage.ErrProtocol)
	}
	// Cookie refresh from signed-download responses is intentionally ignored: those
	// endpoints do not own the API credential lifecycle or its persistence.
	verify := func() error {
		current, err := p.fileInfo(readCtx, f.ID)
		if err != nil {
			return err
		}
		if current.Size != f.Size || current.UpdatedAt != f.UpdatedAt || current.MD5 != f.MD5 || current.SHA1 != f.SHA1 {
			return cloudstorage.ErrConflict
		}
		return nil
	}
	return &validatedReader{body: resp.Body, cancel: cancel, ctx: readCtx, remaining: want, verify: verify}, nil
}

func (p *Provider) downloadHost(u *url.URL) bool {
	return strings.HasSuffix(strings.ToLower(u.Hostname()), ".quark.cn") || (p.allowLoopback && isLoopback(u.Hostname()))
}

type validatedReader struct {
	body      io.ReadCloser
	cancel    context.CancelFunc
	ctx       context.Context
	remaining int64
	done      bool
	verify    func() error
	terminal  error
}

func (r *validatedReader) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if r.done {
		return 0, r.terminal
	}
	if r.remaining == 0 {
		var extra [1]byte
		n, err := r.body.Read(extra[:])
		if n > 0 {
			return 0, r.fail(cloudstorage.ErrSizeMismatch)
		}
		if errors.Is(err, io.EOF) {
			return 0, r.finish()
		}
		if err != nil {
			return 0, r.fail(safeTransport(r.ctx, err))
		}
		return 0, nil
	}
	p = p[:min(int64(len(p)), r.remaining)]
	n, err := r.body.Read(p)
	r.remaining -= int64(n)
	if errors.Is(err, io.EOF) && r.remaining > 0 {
		return n, r.fail(io.ErrUnexpectedEOF)
	}
	if errors.Is(err, io.EOF) {
		return n, r.finish()
	}
	if err != nil && !errors.Is(err, io.EOF) {
		return n, r.fail(safeTransport(r.ctx, err))
	}
	return n, err
}

func (r *validatedReader) fail(err error) error {
	r.done, r.terminal = true, err
	return err
}

func (r *validatedReader) finish() error {
	r.done = true
	r.terminal = io.EOF
	if r.verify != nil {
		if err := r.verify(); err != nil {
			r.terminal = err
		}
	}
	return r.terminal
}
func (r *validatedReader) Close() error {
	r.cancel()
	if err := r.body.Close(); err != nil {
		return safeTransport(context.WithoutCancel(r.ctx), err)
	}
	return nil
}
