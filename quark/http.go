package quark

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	cloudstorage "github.com/Niexiawei/simple-cloud-storage"
)

type envelope struct {
	Success  *bool           `json:"success"` // /account/info uses a separate response contract.
	Status   *int            `json:"status"`
	Code     *int            `json:"code"`
	Message  string          `json:"message"`
	Data     json.RawMessage `json:"data"`
	Metadata json.RawMessage `json:"metadata"`
}

func (p *Provider) call(ctx context.Context, method, path string, query url.Values, body, out any) (retErr error) {
	defer func() {
		if retErr != nil {
			retErr = fmt.Errorf("quark %s %s: %w", method, path, retErr)
		}
	}()
	err := p.request(ctx, method, p.api+path, query, body, out, "", false)
	safeToRepeat := method == http.MethodGet || path == "/file/update/hash" || path == "/file/upload/auth" || path == "/file/download"
	if errors.Is(err, cloudstorage.ErrAuthentication) && path != "/config" && safeToRepeat {
		if refreshErr := p.RefreshCookies(ctx); refreshErr != nil {
			return refreshErr
		}
		return p.request(ctx, method, p.api+path, query, body, out, "", false)
	}
	return err
}

func (p *Provider) request(ctx context.Context, method, rawURL string, query url.Values, body, out any, cookie string, refresh bool) error {
	if p.closed.Load() {
		return fmt.Errorf("quark connection is closed: %w", cloudstorage.ErrInvalidArgument)
	}
	u, err := url.Parse(rawURL)
	if err != nil {
		return cloudstorage.ErrProtocol
	}
	q := u.Query()
	if strings.HasPrefix(rawURL, p.api+"/") {
		q.Set("pr", "ucpro")
		q.Set("fr", "pc")
	}
	for k, vs := range query {
		q[k] = vs
	}
	u.RawQuery = q.Encode()
	var data []byte
	if body != nil {
		data, err = json.Marshal(body)
		if err != nil {
			return err
		}
	}
	attempts := 1
	if method == http.MethodGet || strings.HasSuffix(u.Path, "/file/upload/auth") || strings.HasSuffix(u.Path, "/file/update/hash") {
		attempts = 3
	}
	for attempt := 0; attempt < attempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		snapshot := cookie
		if snapshot == "" {
			snapshot = p.cookies.snapshot()
		}
		outgoing := snapshot
		if refresh {
			values, err := parseCookies(snapshot)
			if err != nil {
				return err
			}
			delete(values, "__puus")
			outgoing = formatCookies(values)
		}
		req, err := http.NewRequestWithContext(ctx, method, u.String(), bytes.NewReader(data))
		if err != nil {
			return cloudstorage.ErrInvalidArgument
		}
		req.GetBody = nil // Do not let HTTP transports replay non-idempotent control calls.
		req.Header.Set("Cookie", outgoing)
		req.Header.Set("Referer", "https://pan.quark.cn")
		req.Header.Set("User-Agent", apiUserAgent)
		if rawURL == p.api+"/file/download" {
			// The reference driver overrides its generic API user agent here.
			// Signed links and their data requests must both use the desktop client.
			req.Header.Set("User-Agent", userAgent)
		}
		req.Header.Set("Accept", "application/json, text/plain, */*")
		if body != nil {
			req.Header.Set("Content-Type", "application/json")
		}
		resp, requestErr := p.http.Do(req)
		var result envelope
		if requestErr != nil {
			err = safeTransport(ctx, requestErr)
		} else {
			raw, readErr := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
			closeErr := resp.Body.Close()
			if readErr != nil || closeErr != nil {
				err = cloudstorage.ErrTemporary
			} else if json.Unmarshal(raw, &result) != nil {
				err = statusError(resp.StatusCode, nil, resp.Header.Get("Retry-After"))
				if err == nil {
					err = responseProtocolError(resp.StatusCode, "invalid response JSON or envelope field types")
				}
			} else {
				if rawURL == p.accountURL {
					err = accountStatusError(resp.StatusCode, &result, resp.Header.Get("Retry-After"))
				} else if strings.HasPrefix(rawURL, p.api+"/") && result.Code == nil {
					err = statusError(resp.StatusCode, nil, resp.Header.Get("Retry-After"))
					if err == nil {
						err = responseProtocolError(resp.StatusCode, "missing response code")
					}
				} else {
					err = statusError(resp.StatusCode, &result, resp.Header.Get("Retry-After"))
				}
				if err == nil {
					if err := p.cookies.merge(resp.Cookies(), snapshot); err != nil {
						return err
					}
					if out != nil {
						if _, ok := out.(*envelope); ok {
							*(out.(*envelope)) = result
							return nil
						}
						if len(result.Data) == 0 || string(result.Data) == "null" || json.Unmarshal(result.Data, out) != nil {
							return responseProtocolError(resp.StatusCode, "missing or invalid response data")
						}
					}
					return nil
				}
			}
		}
		if attempt+1 == attempts || (!errors.Is(err, cloudstorage.ErrTemporary) && !errors.Is(err, cloudstorage.ErrRateLimited)) {
			return err
		}
		delay := retryDelay(err, attempt)
		if err := wait(ctx, delay); err != nil {
			return err
		}
	}
	return cloudstorage.ErrTemporary
}

func responseProtocolError(httpStatus int, reason string) error {
	// Keep diagnostics actionable without copying response bodies, account data,
	// cookies, redirects or signed URLs into errors.
	return &cloudstorage.Error{Op: "quark api", Provider: "quark", Kind: cloudstorage.ErrProtocol, HTTPStatus: httpStatus,
		Err: fmt.Errorf("http status=%d: %s", httpStatus, reason)}
}

func accountStatusError(httpStatus int, result *envelope, retryAfter string) error {
	if err := statusError(httpStatus, nil, retryAfter); err != nil {
		return err
	}
	// The account service signals success with a boolean, independently of the
	// drive API's status=200/code=0. Never apply its numeric status namespace here.
	if result.Success == nil {
		return responseProtocolError(httpStatus, "missing account success flag")
	}
	if !*result.Success {
		return cloudstorage.ErrAuthentication
	}
	return nil
}

func statusError(httpStatus int, result *envelope, retryAfter string) error {
	status, code := httpStatus, 0
	if result != nil {
		if result.Status == nil {
			return responseProtocolError(httpStatus, "missing response status")
		}
		status = *result.Status
		if result.Code != nil {
			code = *result.Code
		}
	}
	if httpStatus == 200 && status == 200 && code == 0 {
		return nil
	}
	kind := cloudstorage.ErrProtocol
	switch {
	case httpStatus == 401 || status == 401:
		kind = cloudstorage.ErrAuthentication
	case httpStatus == 403 || status == 403:
		kind = cloudstorage.ErrPermission
	case httpStatus == 429 || status == 429:
		kind = cloudstorage.ErrRateLimited
	case httpStatus >= 500 || status >= 500:
		kind = cloudstorage.ErrTemporary
	case httpStatus == 404 && status == 404 && code == 21001:
		kind = cloudstorage.ErrNotFound
	case httpStatus == 409 || status == 409:
		kind = cloudstorage.ErrConflict
	case result != nil && strings.Contains(result.Message, "同名冲突"):
		kind = cloudstorage.ErrConflict
	case result != nil && (strings.Contains(strings.ToLower(result.Message), "login") || strings.Contains(result.Message, "登录")):
		kind = cloudstorage.ErrAuthentication
	}
	delay := time.Duration(0)
	if seconds, err := strconv.ParseInt(retryAfter, 10, 64); err == nil && seconds > 0 && seconds <= int64((1<<63-1)/int64(time.Second)) {
		delay = time.Duration(seconds) * time.Second
	} else if deadline, err := http.ParseTime(retryAfter); err == nil {
		delay = max(0, time.Until(deadline))
	}
	return &cloudstorage.Error{Op: "quark api", Provider: "quark", Kind: kind, HTTPStatus: httpStatus, RetryAfter: delay, Err: fmt.Errorf("provider status=%d code=%d", status, code)}
}

func retryDelay(err error, attempt int) time.Duration {
	delay := time.Duration(1<<attempt) * 250 * time.Millisecond
	var providerErr *cloudstorage.Error
	if errors.As(err, &providerErr) && providerErr.RetryAfter > delay {
		delay = providerErr.RetryAfter
	}
	return delay
}

func safeTransport(ctx context.Context, err error) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if errors.Is(err, context.Canceled) {
		return context.Canceled
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return context.DeadlineExceeded
	}
	if errors.Is(err, cloudstorage.ErrProtocol) {
		return cloudstorage.ErrProtocol
	}
	// url.Error may contain signed URLs or sensitive query values; never expose it.
	return fmt.Errorf("quark transport failed: %w", cloudstorage.ErrTemporary)
}

func wait(ctx context.Context, delay time.Duration) error {
	t := time.NewTimer(delay)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

func isLoopback(host string) bool {
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}
