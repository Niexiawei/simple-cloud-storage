package quark

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/base64"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"html"
	"io"
	"mime"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
	"time"

	cloudstorage "github.com/Niexiawei/simple-cloud-storage"
)

const ossAgent = "aliyun-sdk-js/6.6.1 Chrome 98.0.4758.80 on Windows 10 64-bit"

type preData struct {
	TaskID    string          `json:"task_id"`
	FileID    string          `json:"fid"`
	UploadID  string          `json:"upload_id"`
	ObjectKey string          `json:"obj_key"`
	Bucket    string          `json:"bucket"`
	UploadURL string          `json:"upload_url"`
	AuthInfo  string          `json:"auth_info"`
	Callback  json.RawMessage `json:"callback"`
	Finish    bool            `json:"finish"`
}

// sessionRecord is credential material, kept in TokenStore rather than checkpoint.
type sessionRecord struct {
	Pre       preData `json:"pre"`
	Path      string  `json:"path"`
	Size      int64   `json:"size"`
	MD5       string  `json:"md5"`
	SHA1      string  `json:"sha1"`
	PartSize  int64   `json:"part_size"`
	Committed bool    `json:"committed"`
	HashDone  bool    `json:"hash_done"`
	Instant   bool    `json:"instant"`
}

type uploadSession struct {
	p             *Provider
	record        sessionRecord
	instant, done bool
	hashDone      bool
}

func (s *uploadSession) PartSize() int64 { return s.record.PartSize }

func (p *Provider) CreateMultipart(ctx context.Context, full string, opts cloudstorage.UploadOptions) (cloudstorage.MultipartSession, error) {
	if opts.Resume {
		return nil, cloudstorage.ErrInvalidArgument
	}
	return p.create(ctx, full, opts)
}

func (p *Provider) create(ctx context.Context, full string, opts cloudstorage.UploadOptions) (*uploadSession, error) {
	if opts.Size == nil || *opts.Size < 0 || !validHex(opts.MD5, 32) || !validHex(opts.SHA1, 40) {
		return nil, cloudstorage.ErrInvalidArgument
	}
	parent, err := p.directory(ctx, path.Dir(full), true)
	if err != nil {
		return nil, fmt.Errorf("quark upload directory: %w", err)
	}
	if _, err := p.find(ctx, parent, path.Base(full)); err == nil {
		return nil, cloudstorage.ErrConflict
	} else if !errors.Is(err, cloudstorage.ErrNotFound) {
		return nil, fmt.Errorf("quark upload check target: %w", err)
	}
	typeName := mime.TypeByExtension(path.Ext(full))
	if typeName == "" {
		typeName = "application/octet-stream"
	}
	var response envelope
	err = p.call(ctx, http.MethodPost, "/file/upload/pre", nil, map[string]any{"ccp_hash_update": true, "dir_name": "", "file_name": path.Base(full), "format_type": typeName, "l_created_at": time.Now().UnixMilli(), "l_updated_at": time.Now().UnixMilli(), "pdir_fid": parent, "size": *opts.Size}, &response)
	if err != nil {
		return nil, err
	}
	var pre preData
	var meta struct {
		PartSize int64 `json:"part_size"`
	}
	if json.Unmarshal(response.Data, &pre) != nil || json.Unmarshal(response.Metadata, &meta) != nil {
		return nil, fmt.Errorf("quark upload pre: invalid data or metadata field types: %w", cloudstorage.ErrProtocol)
	}
	for _, field := range []struct{ name, value string }{
		{"task_id", pre.TaskID}, {"fid", pre.FileID}, {"upload_id", pre.UploadID},
		{"obj_key", pre.ObjectKey}, {"bucket", pre.Bucket}, {"auth_info", pre.AuthInfo},
	} {
		if field.value == "" {
			return nil, fmt.Errorf("quark upload pre: missing %s: %w", field.name, cloudstorage.ErrProtocol)
		}
	}
	if meta.PartSize <= 0 || meta.PartSize > 128<<20 {
		return nil, fmt.Errorf("quark upload pre: invalid part_size=%d: %w", meta.PartSize, cloudstorage.ErrProtocol)
	}
	if _, err := ossURL(pre, p.allowLoopback); err != nil {
		return nil, fmt.Errorf("quark upload pre: invalid OSS endpoint: %w", err)
	}
	s := &uploadSession{p: p, record: sessionRecord{Pre: pre, Path: full, Size: *opts.Size, MD5: opts.MD5, SHA1: opts.SHA1, PartSize: meta.PartSize}}
	// Persist PRE once before issuing HASH or any body. An error never replays PRE.
	if err := s.saveSecret(ctx); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *uploadSession) secretKey() string {
	return "quark-upload/" + s.p.account + "/" + s.record.Pre.TaskID
}
func (s *uploadSession) saveSecret(ctx context.Context) error {
	data, err := json.Marshal(s.record)
	if err != nil {
		return err
	}
	return s.p.secrets.Save(ctx, s.secretKey(), cloudstorage.Token{Extra: map[string]string{"session": string(data)}})
}

func (s *uploadSession) updateHash(ctx context.Context) error {
	if s.record.Committed {
		return nil
	}
	if s.record.HashDone {
		s.instant = s.record.Instant
		s.hashDone = true
		return nil
	}
	if s.hashDone {
		return nil
	}
	var result struct {
		Finish bool   `json:"finish"`
		ID     string `json:"fid"`
	}
	err := s.p.call(ctx, http.MethodPost, "/file/update/hash", nil, map[string]any{"md5": s.record.MD5, "sha1": s.record.SHA1, "task_id": s.record.Pre.TaskID}, &result)
	if err != nil {
		return err
	}
	if result.ID != "" && result.ID != s.record.Pre.FileID {
		return fmt.Errorf("quark upload hash: response fid differs from pre: %w", cloudstorage.ErrProtocol)
	}
	s.instant = result.Finish || s.record.Pre.Finish
	s.hashDone = true
	s.record.HashDone, s.record.Instant = true, s.instant
	return s.saveSecret(ctx)
}

func (p *Provider) Upload(ctx context.Context, full string, r io.Reader, opts cloudstorage.UploadOptions) (info cloudstorage.ObjectInfo, retErr error) {
	if opts.Resume {
		return info, cloudstorage.ErrInvalidArgument
	}
	s, err := p.create(ctx, full, opts)
	if err != nil {
		return cloudstorage.ObjectInfo{}, err
	}
	defer func() {
		if retErr != nil {
			cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), cloudstorage.DefaultCleanupTimeout)
			defer cancel()
			retErr = errors.Join(retErr, s.Abort(cleanupCtx))
		}
	}()
	if err := s.updateHash(ctx); err != nil {
		return cloudstorage.ObjectInfo{}, err
	}
	var parts []cloudstorage.PartInfo
	if !s.instant {
		buffer := make([]byte, int(s.record.PartSize))
		for remaining, number := *opts.Size, 1; remaining > 0; number++ {
			want := min(s.record.PartSize, remaining)
			if _, err := io.ReadFull(r, buffer[:want]); err != nil {
				return cloudstorage.ObjectInfo{}, errors.Join(cloudstorage.ErrSizeMismatch, err)
			}
			part, err := s.UploadPart(ctx, number, bytes.NewReader(buffer[:want]), want)
			if err != nil {
				return cloudstorage.ObjectInfo{}, err
			}
			parts = append(parts, part)
			remaining -= want
		}
	}
	return s.Complete(ctx, parts)
}

func (s *uploadSession) UploadPart(ctx context.Context, number int, r io.Reader, size int64) (cloudstorage.PartInfo, error) {
	part := cloudstorage.PartInfo{PartNumber: number, Size: size}
	if err := s.updateHash(ctx); err != nil {
		return part, err
	}
	if s.instant {
		part.ETag = "instant"
		return part, nil
	}
	total := int64(0)
	if s.record.Size > 0 {
		total = 1 + (s.record.Size-1)/s.record.PartSize
	}
	if number < 1 || int64(number) > total || size != min(s.record.PartSize, s.record.Size-(int64(number)-1)*s.record.PartSize) {
		return part, cloudstorage.ErrInvalidArgument
	}
	var source io.ReadSeeker
	if seeker, ok := r.(io.ReadSeeker); ok {
		source = seeker
	} else {
		data, err := io.ReadAll(io.LimitReader(r, size+1))
		if err != nil {
			return part, err
		}
		if int64(len(data)) != size {
			return part, cloudstorage.ErrSizeMismatch
		}
		source = bytes.NewReader(data)
	}
	start, err := source.Seek(0, io.SeekCurrent)
	if err != nil {
		return part, err
	}
	for attempt := 0; attempt < 3; attempt++ {
		if _, err := source.Seek(start, io.SeekStart); err != nil {
			return part, err
		}
		query := url.Values{"partNumber": {strconv.Itoa(number)}, "uploadId": {s.record.Pre.UploadID}}
		resp, err := s.oss(ctx, http.MethodPut, query, source, size, nil)
		if err == nil {
			part.ETag = resp.Header.Get("ETag")
			closeErr := resp.Body.Close()
			if closeErr != nil {
				return part, cloudstorage.ErrTemporary
			}
			if part.ETag == "" {
				return part, cloudstorage.ErrProtocol
			}
			return part, nil
		}
		if !errors.Is(err, cloudstorage.ErrTemporary) && !errors.Is(err, cloudstorage.ErrRateLimited) && !errors.Is(err, cloudstorage.ErrAuthentication) {
			return part, err
		}
		if attempt == 2 {
			return part, err
		}
		if err := wait(ctx, retryDelay(err, attempt)); err != nil {
			return part, err
		}
	}
	return part, cloudstorage.ErrTemporary
}

func (s *uploadSession) oss(ctx context.Context, method string, query url.Values, body io.Reader, size int64, extra http.Header) (*http.Response, error) {
	endpoint, err := ossURL(s.record.Pre, s.p.allowLoopback)
	if err != nil {
		return nil, err
	}
	date := time.Now().UTC().Format(http.TimeFormat)
	contentType := mime.TypeByExtension(path.Ext(s.record.Path))
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	if method != http.MethodPut {
		contentType = ""
	}
	if extra.Get("Content-Type") != "" {
		contentType = extra.Get("Content-Type")
	}
	canonical := fmt.Sprintf("%s\n%s\n%s\n%s\n", method, extra.Get("Content-MD5"), contentType, date)
	if callback := extra.Get("x-oss-callback"); callback != "" {
		canonical += "x-oss-callback:" + callback + "\n"
	}
	canonical += "x-oss-date:" + date + "\nx-oss-user-agent:" + ossAgent + "\n/" + s.record.Pre.Bucket + "/" + s.record.Pre.ObjectKey + "?" + query.Encode()
	var auth struct {
		Key string `json:"auth_key"`
	}
	if err := s.p.call(ctx, http.MethodPost, "/file/upload/auth", nil, map[string]any{"auth_info": s.record.Pre.AuthInfo, "auth_meta": canonical, "task_id": s.record.Pre.TaskID}, &auth); err != nil {
		return nil, err
	}
	if auth.Key == "" {
		return nil, cloudstorage.ErrProtocol
	}
	endpoint.RawQuery = query.Encode()
	req, err := http.NewRequestWithContext(ctx, method, endpoint.String(), body)
	if err != nil {
		return nil, cloudstorage.ErrProtocol
	}
	req.GetBody = nil
	req.ContentLength = size
	req.Header = extra.Clone()
	if req.Header == nil {
		req.Header = make(http.Header)
	}
	req.Header.Set("Authorization", auth.Key)
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	req.Header.Set("x-oss-date", date)
	req.Header.Set("x-oss-user-agent", ossAgent)
	req.Header.Set("Referer", "https://pan.quark.cn/")
	resp, err := s.p.transfer.Do(req)
	if err != nil {
		return nil, safeTransport(ctx, err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		defer resp.Body.Close()
		var result struct {
			Code string `xml:"Code"`
		}
		xml.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&result)
		if result.Code == "NoSuchUpload" {
			return nil, cloudstorage.ErrSessionExpired
		}
		if result.Code == "SignatureDoesNotMatch" || result.Code == "AccessDenied" || result.Code == "SecurityTokenExpired" {
			return nil, cloudstorage.ErrAuthentication
		}
		return nil, statusError(resp.StatusCode, nil, resp.Header.Get("Retry-After"))
	}
	return resp, nil
}

func ossURL(pre preData, allowLoopback bool) (*url.URL, error) {
	u, err := url.Parse(pre.UploadURL)
	if err != nil || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") || (u.Scheme != "http" && u.Scheme != "https") {
		return nil, cloudstorage.ErrProtocol
	}
	if !isLoopback(u.Hostname()) {
		host := strings.ToLower(u.Hostname())
		trusted := strings.HasSuffix(host, ".aliyuncs.com") || host == "pds.quark.cn" || strings.HasSuffix(host, ".pds.quark.cn")
		if !trusted || strings.ContainsAny(pre.Bucket, "./:?#@") {
			return nil, cloudstorage.ErrProtocol
		}
		u.Scheme = "https"
		if !strings.HasPrefix(u.Host, pre.Bucket+".") {
			u.Host = pre.Bucket + "." + u.Host
		}
	} else if !allowLoopback || (u.Scheme != "http" && u.Scheme != "https") {
		return nil, cloudstorage.ErrProtocol
	}
	u.Path = "/" + pre.ObjectKey
	u.RawPath = ""
	return u, nil
}

func validHex(value string, width int) bool {
	if len(value) != width {
		return false
	}
	for _, c := range value {
		if !strings.ContainsRune("0123456789abcdefABCDEF", c) {
			return false
		}
	}
	return true
}

type completePart struct {
	Number int    `xml:"PartNumber"`
	ETag   string `xml:"ETag"`
}
type completeBody struct {
	XMLName xml.Name       `xml:"CompleteMultipartUpload"`
	Parts   []completePart `xml:"Part"`
}

func (s *uploadSession) Complete(ctx context.Context, parts []cloudstorage.PartInfo) (cloudstorage.ObjectInfo, error) {
	if s.done {
		return s.confirm(ctx)
	}
	if err := s.updateHash(ctx); err != nil {
		return cloudstorage.ObjectInfo{}, err
	}
	if !s.instant && !s.record.Committed {
		state := cloudstorage.UploadState{Size: s.record.Size, SHA256: strings.Repeat("0", 64), PartSize: s.record.PartSize, Completed: parts}
		if state.Size > 0 {
			state.TotalParts = 1 + (state.Size-1)/state.PartSize
		}
		if err := state.Validate(); err != nil {
			return cloudstorage.ObjectInfo{}, err
		}
		if int64(len(parts)) != state.TotalParts {
			return cloudstorage.ObjectInfo{}, cloudstorage.ErrSizeMismatch
		}
		body := completeBody{}
		for i, part := range parts {
			if part.PartNumber != i+1 || part.ETag == "" {
				return cloudstorage.ObjectInfo{}, cloudstorage.ErrProtocol
			}
			body.Parts = append(body.Parts, completePart{Number: part.PartNumber, ETag: part.ETag})
		}
		encoded, err := xml.Marshal(body)
		if err != nil {
			return cloudstorage.ObjectInfo{}, err
		}
		encoded = append([]byte(xml.Header), encoded...)
		sum := md5.Sum(encoded)
		headers := http.Header{"Content-Type": {"application/xml"}, "Content-Md5": {base64.StdEncoding.EncodeToString(sum[:])}, "X-Oss-Callback": {base64.StdEncoding.EncodeToString(s.record.Pre.Callback)}}
		resp, commitErr := s.oss(ctx, http.MethodPost, url.Values{"uploadId": {s.record.Pre.UploadID}}, bytes.NewReader(encoded), int64(len(encoded)), headers)
		if resp != nil {
			resp.Body.Close()
		}
		if commitErr != nil {
			if info, err := s.confirm(ctx); err == nil {
				return s.succeed(ctx, info)
			}
			// A consumed OSS upload ID may be a successful Commit whose response
			// was lost. Finish only the original task, then verify its exact FID.
			if errors.Is(commitErr, cloudstorage.ErrSessionExpired) {
				finishErr := s.p.call(ctx, http.MethodPost, "/file/upload/finish", nil, map[string]any{"obj_key": s.record.Pre.ObjectKey, "task_id": s.record.Pre.TaskID}, nil)
				if info, checkErr := s.confirm(ctx); checkErr == nil {
					return s.succeed(ctx, info)
				}
				return cloudstorage.ObjectInfo{}, errors.Join(commitErr, finishErr)
			}
			return cloudstorage.ObjectInfo{}, commitErr
		}
		s.record.Committed = true
		if err := s.saveSecret(ctx); err != nil {
			return cloudstorage.ObjectInfo{}, err
		}
	}
	if !s.instant {
		err := s.p.call(ctx, http.MethodPost, "/file/upload/finish", nil, map[string]any{"obj_key": s.record.Pre.ObjectKey, "task_id": s.record.Pre.TaskID}, nil)
		if err != nil {
			if info, checkErr := s.confirm(ctx); checkErr == nil {
				return s.succeed(ctx, info)
			}
			return cloudstorage.ObjectInfo{}, err
		}
	}
	var confirmErr error
	for attempt := 0; attempt < 5; attempt++ {
		info, err := s.confirm(ctx)
		if err == nil {
			return s.succeed(ctx, info)
		}
		if !errors.Is(err, cloudstorage.ErrNotFound) && !errors.Is(err, cloudstorage.ErrTemporary) && !errors.Is(err, cloudstorage.ErrProtocol) {
			return cloudstorage.ObjectInfo{}, err
		}
		confirmErr = err
		if err := wait(ctx, time.Duration(attempt+1)*200*time.Millisecond); err != nil {
			return cloudstorage.ObjectInfo{}, err
		}
	}
	return cloudstorage.ObjectInfo{}, fmt.Errorf("quark upload confirm after retries: %w", confirmErr)
}

func (s *uploadSession) succeed(ctx context.Context, info cloudstorage.ObjectInfo) (cloudstorage.ObjectInfo, error) {
	s.done = true
	return info, ignoreNotFound(s.p.secrets.Delete(ctx, s.secretKey()))
}

func (s *uploadSession) confirm(ctx context.Context) (cloudstorage.ObjectInfo, error) {
	f, err := s.p.fileInfo(ctx, s.record.Pre.FileID)
	if err != nil {
		return cloudstorage.ObjectInfo{}, err
	}
	if f.Size != s.record.Size || (f.MD5 != "" && !strings.EqualFold(f.MD5, s.record.MD5)) || (f.SHA1 != "" && !strings.EqualFold(f.SHA1, s.record.SHA1)) {
		return cloudstorage.ObjectInfo{}, cloudstorage.ErrSizeMismatch
	}
	if html.UnescapeString(f.Name) != path.Base(s.record.Path) {
		return cloudstorage.ObjectInfo{}, cloudstorage.ErrConflict
	}
	r, err := s.p.openFile(ctx, f, cloudstorage.ReadOptions{Length: 1})
	if err != nil {
		return cloudstorage.ObjectInfo{}, err
	}
	var one [1]byte
	_, readErr := r.Read(one[:])
	closeErr := r.Close()
	if readErr != nil && !errors.Is(readErr, io.EOF) {
		return cloudstorage.ObjectInfo{}, readErr
	}
	if closeErr != nil {
		return cloudstorage.ObjectInfo{}, closeErr
	}
	return f.info(), nil
}

func (s *uploadSession) Abort(ctx context.Context) error {
	if s.done {
		return ignoreNotFound(s.p.secrets.Delete(ctx, s.secretKey()))
	}
	if !s.record.Committed && !s.instant {
		resp, err := s.oss(ctx, http.MethodDelete, url.Values{"uploadId": {s.record.Pre.UploadID}}, nil, 0, nil)
		if resp != nil {
			resp.Body.Close()
		}
		if err != nil && !errors.Is(err, cloudstorage.ErrSessionExpired) {
			return err
		}
	}
	return ignoreNotFound(s.p.secrets.Delete(ctx, s.secretKey()))
}
