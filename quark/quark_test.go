package quark

import (
	"bufio"
	"context"
	"crypto/md5"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	cloudstorage "github.com/Niexiawei/simple-cloud-storage"
)

type fakeUpload struct {
	ID, Parent, Name    string
	Size                int64
	Parts               map[int][]byte
	Committed, Finished bool
}
type fakeQuark struct {
	mu                                sync.Mutex
	server                            *httptest.Server
	files                             map[string]file
	children                          map[string][]string
	data                              map[string][]byte
	uploads                           map[string]*fakeUpload
	preCalls, finishCalls, mkdirCalls int
	partCalls                         map[int]int
	failPart                          int
	loseFinish                        bool
	badRange                          string
	breakDownload                     bool
	lastRange                         string
	refreshes                         int
	instant                           bool
	downloadCookies                   map[string]string
}

func newFake(t *testing.T) *fakeQuark {
	t.Helper()
	f := &fakeQuark{files: make(map[string]file), children: make(map[string][]string), data: make(map[string][]byte), uploads: make(map[string]*fakeUpload), partCalls: make(map[int]int), downloadCookies: make(map[string]string)}
	f.server = httptest.NewUnstartedServer(http.HandlerFunc(f.serve))
	f.server.URL = "http://127.0.0.1:9876"
	return f
}

func (f *fakeQuark) cfg(t *testing.T) Config {
	t.Helper()
	dir := t.TempDir()
	return Config{Cookies: "__pus=bootstrap; __puus=old", CookieFile: filepath.Join(dir, "cookies.json"), StateDir: filepath.Join(dir, "state"), APIBaseURL: f.server.URL + "/api", DisableAutoRefresh: true, HTTPClient: &http.Client{Transport: fakeTransport{f}}}
}

func (f *fakeQuark) serve(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	write := func(status, code int, data any) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		json.NewEncoder(w).Encode(map[string]any{"status": status, "code": code, "data": data})
	}
	decode := func() map[string]any { var body map[string]any; json.NewDecoder(r.Body).Decode(&body); return body }
	if r.URL.Path == "/account" {
		if !strings.Contains(r.Header.Get("Cookie"), "__pus=bootstrap") {
			write(401, 1, nil)
			return
		}
		json.NewEncoder(w).Encode(map[string]any{"success": true, "data": map[string]any{"uid": "account"}})
		return
	}
	if r.URL.Path == "/api/config" {
		if _, err := r.Cookie("__puus"); err == nil {
			write(400, 1, nil)
			return
		}
		f.refreshes++
		http.SetCookie(w, &http.Cookie{Name: "__puus", Value: fmt.Sprintf("fresh-%d", f.refreshes), Path: "/"})
		write(200, 0, map[string]any{})
		return
	}
	if strings.HasPrefix(r.URL.Path, "/api/") && r.URL.Query().Get("pr") != "ucpro" {
		write(400, 1, nil)
		return
	}
	switch r.URL.Path {
	case "/api/file/sort":
		parent := r.URL.Query().Get("pdir_fid")
		ids := f.children[parent]
		page, _ := strconv.Atoi(r.URL.Query().Get("_page"))
		var files []file
		for i := (page - 1) * 100; i < len(ids) && i < page*100; i++ {
			files = append(files, f.files[ids[i]])
		}
		if files == nil {
			files = []file{}
		}
		json.NewEncoder(w).Encode(map[string]any{"status": 200, "code": 0, "data": map[string]any{"list": files}, "metadata": map[string]any{"_total": len(ids)}})
	case "/api/file":
		body := decode()
		parent := body["pdir_fid"].(string)
		name := body["file_name"].(string)
		for _, id := range f.children[parent] {
			if f.files[id].Name == name {
				write(409, 1, nil)
				return
			}
		}
		f.mkdirCalls++
		id := fmt.Sprintf("dir-%d", f.mkdirCalls)
		f.files[id] = file{ID: id, Name: name}
		f.children[parent] = append(f.children[parent], id)
		write(200, 0, map[string]any{"fid": id})
	case "/api/file/info":
		id := r.URL.Query().Get("fid")
		item, ok := f.files[id]
		if !ok {
			write(404, 21001, nil)
			return
		}
		write(200, 0, item)
	case "/api/file/delete":
		body := decode()
		for _, value := range body["filelist"].([]any) {
			id := value.(string)
			delete(f.files, id)
			delete(f.data, id)
			for parent, ids := range f.children {
				var remaining []string
				for _, child := range ids {
					if child != id {
						remaining = append(remaining, child)
					}
				}
				f.children[parent] = remaining
			}
		}
		write(200, 0, map[string]any{})
	case "/api/file/upload/pre":
		body := decode()
		f.preCalls++
		task := fmt.Sprintf("task-%d", f.preCalls)
		id := "file-" + task
		u := &fakeUpload{ID: id, Parent: body["pdir_fid"].(string), Name: body["file_name"].(string), Size: int64(body["size"].(float64)), Parts: make(map[int][]byte)}
		f.uploads[task] = u
		f.files[id] = file{ID: id, Name: u.Name, Size: u.Size, IsFile: true, UpdatedAt: 1000}
		json.NewEncoder(w).Encode(map[string]any{"status": 200, "code": 0, "data": map[string]any{"task_id": task, "fid": id, "upload_id": "upload-" + task, "obj_key": "object/" + task, "bucket": "bucket", "upload_url": f.server.URL, "auth_info": "private-auth", "callback": map[string]string{"callbackUrl": "https://callback.quark.cn", "callbackBody": "private-callback"}}, "metadata": map[string]any{"part_size": 4}})
	case "/api/file/update/hash":
		body := decode()
		if f.instant {
			u := f.uploads[body["task_id"].(string)]
			u.Committed, u.Finished = true, true
			f.data[u.ID] = []byte("abcd")
			f.children[u.Parent] = append(f.children[u.Parent], u.ID)
		}
		write(200, 0, map[string]any{"finish": f.instant})
	case "/api/file/upload/auth":
		body := decode()
		if body["auth_info"] != "private-auth" || !strings.Contains(body["auth_meta"].(string), "x-oss-user-agent:"+ossAgent) {
			write(400, 1, nil)
			return
		}
		write(200, 0, map[string]any{"auth_key": "signed-auth"})
	case "/api/file/upload/finish":
		body := decode()
		f.finishCalls++
		u := f.uploads[body["task_id"].(string)]
		if u == nil || !u.Committed {
			write(400, 1, nil)
			return
		}
		u.Finished = true
		f.children[u.Parent] = append(f.children[u.Parent], u.ID)
		if f.loseFinish {
			f.loseFinish = false
			conn, _, _ := w.(http.Hijacker).Hijack()
			conn.Close()
			return
		}
		write(200, 0, map[string]any{})
	case "/api/file/download":
		body := decode()
		id := body["fids"].([]any)[0].(string)
		data, ok := f.data[id]
		finished := false
		for _, u := range f.uploads {
			if u.ID == id {
				finished = u.Finished
			}
		}
		if !ok || !finished {
			write(404, 21001, nil)
			return
		}
		_ = data
		f.downloadCookies[id] = r.Header.Get("Cookie")
		// Rotate during URL generation: the following GET must keep this request's snapshot.
		http.SetCookie(w, &http.Cookie{Name: "__puus", Value: "download-rotated", Path: "/"})
		write(200, 0, []map[string]any{{"fid": id, "download_url": f.server.URL + "/blob/" + id}})
	default:
		if strings.HasPrefix(r.URL.Path, "/object/") {
			if r.Header.Get("Cookie") != "" || r.Header.Get("Authorization") != "signed-auth" {
				write(403, 1, nil)
				return
			}
			task := strings.TrimPrefix(r.URL.Path, "/object/")
			u := f.uploads[task]
			if u == nil {
				w.WriteHeader(404)
				io.WriteString(w, "<Error><Code>NoSuchUpload</Code></Error>")
				return
			}
			switch r.Method {
			case http.MethodPut:
				number, _ := strconv.Atoi(r.URL.Query().Get("partNumber"))
				f.partCalls[number]++
				if f.failPart == number {
					w.WriteHeader(503)
					return
				}
				data, _ := io.ReadAll(r.Body)
				u.Parts[number] = data
				w.Header().Set("ETag", fmt.Sprintf("\"etag-%d\"", number))
				w.WriteHeader(200)
			case http.MethodPost:
				if u.Committed {
					w.WriteHeader(404)
					io.WriteString(w, "<Error><Code>NoSuchUpload</Code></Error>")
					return
				}
				data, _ := io.ReadAll(r.Body)
				sum := md5.Sum(data)
				if r.Header.Get("Content-MD5") != base64.StdEncoding.EncodeToString(sum[:]) {
					write(400, 1, nil)
					return
				}
				var complete completeBody
				if xml.Unmarshal(data, &complete) != nil {
					write(400, 1, nil)
					return
				}
				var numbers []int
				for number := range u.Parts {
					numbers = append(numbers, number)
				}
				sort.Ints(numbers)
				var content []byte
				for _, number := range numbers {
					content = append(content, u.Parts[number]...)
				}
				if int64(len(content)) != u.Size || len(complete.Parts) != len(numbers) {
					write(400, 1, nil)
					return
				}
				u.Committed = true
				f.data[u.ID] = content
				item := f.files[u.ID]
				hash := md5.Sum(content)
				item.MD5 = hex.EncodeToString(hash[:])
				f.files[u.ID] = item
				w.WriteHeader(200)
				io.WriteString(w, "<CompleteMultipartUploadResult/>")
			case http.MethodDelete:
				delete(f.uploads, task)
				write(200, 0, map[string]any{})
			}
			return
		}
		if strings.HasPrefix(r.URL.Path, "/blob/") {
			id := strings.TrimPrefix(r.URL.Path, "/blob/")
			data := f.data[id]
			if r.Header.Get("Cookie") != f.downloadCookies[id] {
				w.WriteHeader(403)
				return
			}
			start, end := int64(0), int64(len(data))-1
			f.lastRange = r.Header.Get("Range")
			if f.lastRange != "" {
				fmt.Sscanf(f.lastRange, "bytes=%d-%d", &start, &end)
			}
			if start < 0 || end >= int64(len(data)) {
				w.WriteHeader(416)
				return
			}
			if f.lastRange != "" && f.badRange != "200" {
				returnedStart := start
				if f.badRange == "start" {
					returnedStart++
				}
				w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", returnedStart, end, len(data)))
			}
			w.Header().Set("Content-Length", strconv.FormatInt(end-start+1, 10))
			if f.lastRange != "" && f.badRange != "200" {
				w.WriteHeader(206)
			} else {
				w.WriteHeader(200)
			}
			if f.breakDownload && end-start > 3 {
				f.breakDownload = false
				w.Write(data[start : start+3])
				w.(http.Flusher).Flush()
				conn, _, _ := w.(http.Hijacker).Hijack()
				conn.Close()
				return
			}
			if len(data) > 0 {
				w.Write(data[start : end+1])
			}
			return
		}
		write(404, 21001, nil)
	}
}

func connectFake(t *testing.T, f *fakeQuark, cfg Config) *Connection {
	t.Helper()
	c, err := Connect(context.Background(), cfg, cloudstorage.Config{Prefix: "apps/test", PartSize: 20, MultipartThreshold: 4, TempDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

func TestQuarkRoundTripAndSignedCookieSnapshot(t *testing.T) {
	f := newFake(t)
	cfg := f.cfg(t)
	c := connectFake(t, f, cfg)
	info, err := c.Upload(context.Background(), "docs/a", strings.NewReader("abcdefghij"), cloudstorage.UploadOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if info.Key != "docs/a" || info.Size != 10 {
		t.Fatalf("info=%+v", info)
	}
	r, err := c.Open(context.Background(), "docs/a", cloudstorage.ReadOptions{Offset: 3, Length: 4})
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(r)
	r.Close()
	if err != nil || string(data) != "defg" {
		t.Fatalf("range=%q %v", data, err)
	}
	f.mu.Lock()
	pre, mkdir := f.preCalls, f.mkdirCalls
	parts := len(f.partCalls)
	f.mu.Unlock()
	if pre != 1 || mkdir != 3 || parts != 3 {
		t.Fatalf("protocol counts pre=%d mkdir=%d parts=%d", pre, mkdir, parts)
	}
	if err := c.Delete(context.Background(), "docs/a"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Stat(context.Background(), "docs/a"); !errors.Is(err, cloudstorage.ErrNotFound) {
		t.Fatal(err)
	}
}

func TestQuarkUploadResumesAcrossConnections(t *testing.T) {
	f := newFake(t)
	cfg := f.cfg(t)
	c := connectFake(t, f, cfg)
	f.mu.Lock()
	f.failPart = 2
	f.mu.Unlock()
	_, err := c.Upload(context.Background(), "docs/a", strings.NewReader("abcdefghij"), cloudstorage.UploadOptions{Resume: true})
	if !errors.Is(err, cloudstorage.ErrTemporary) {
		t.Fatalf("first upload=%v", err)
	}
	c.Close()
	checkpoints, err := os.ReadDir(filepath.Join(cfg.StateDir, "checkpoints"))
	if err != nil || len(checkpoints) != 1 {
		t.Fatalf("checkpoint=%v %v", checkpoints, err)
	}
	checkpointBytes, err := os.ReadFile(filepath.Join(cfg.StateDir, "checkpoints", checkpoints[0].Name()))
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{"private-auth", "signed-auth", "private-callback", "bootstrap", "http://"} {
		if strings.Contains(string(checkpointBytes), secret) {
			t.Fatalf("checkpoint leaked %q", secret)
		}
	}
	f.mu.Lock()
	f.failPart = 0
	f.mu.Unlock()
	c = connectFake(t, f, cfg)
	info, err := c.Upload(context.Background(), "docs/a", strings.NewReader("abcdefghij"), cloudstorage.UploadOptions{Resume: true})
	if err != nil || info.Size != 10 {
		t.Fatalf("resume=%+v %v", info, err)
	}
	f.mu.Lock()
	pre, partOne := f.preCalls, f.partCalls[1]
	f.mu.Unlock()
	if pre != 1 || partOne != 1 {
		t.Fatalf("resume created a new task or repeated confirmed part: pre=%d part1=%d", pre, partOne)
	}
	checkpoints, err = os.ReadDir(filepath.Join(cfg.StateDir, "checkpoints"))
	if err != nil || len(checkpoints) != 0 {
		t.Fatalf("checkpoint not cleared %v %v", checkpoints, err)
	}
}

func TestQuarkDownloadResumesAndRejectsWrongRange(t *testing.T) {
	f := newFake(t)
	cfg := f.cfg(t)
	c := connectFake(t, f, cfg)
	if _, err := c.Upload(context.Background(), "a", strings.NewReader("abcdefghij"), cloudstorage.UploadOptions{}); err != nil {
		t.Fatal(err)
	}
	dst := filepath.Join(t.TempDir(), "download")
	f.mu.Lock()
	f.breakDownload = true
	f.mu.Unlock()
	if err := c.DownloadToFile(context.Background(), "a", dst, cloudstorage.DownloadOptions{Resume: true}); err == nil {
		t.Fatal("interrupted download succeeded")
	}
	if data, err := os.ReadFile(dst + ".part"); err != nil || string(data) != "abc" {
		t.Fatalf("partial=%q %v", data, err)
	}
	c.Close()
	c = connectFake(t, f, cfg)
	for _, mode := range []string{"200", "start"} {
		f.mu.Lock()
		f.badRange = mode
		f.mu.Unlock()
		if err := c.DownloadToFile(context.Background(), "a", dst, cloudstorage.DownloadOptions{Resume: true}); !errors.Is(err, cloudstorage.ErrProtocol) {
			t.Fatalf("bad range %s accepted: %v", mode, err)
		}
		if data, err := os.ReadFile(dst + ".part"); err != nil || string(data) != "abc" {
			t.Fatal("invalid range appended")
		}
	}
	f.mu.Lock()
	f.badRange = ""
	f.mu.Unlock()
	if err := c.DownloadToFile(context.Background(), "a", dst, cloudstorage.DownloadOptions{Resume: true}); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(dst)
	if err != nil || string(data) != "abcdefghij" {
		t.Fatalf("download=%q %v", data, err)
	}
	f.mu.Lock()
	lastRange := f.lastRange
	f.mu.Unlock()
	if lastRange != "bytes=3-9" {
		t.Fatalf("range=%q", lastRange)
	}
}

func TestLostFinishResponseIsConfirmedWithoutReplay(t *testing.T) {
	f := newFake(t)
	cfg := f.cfg(t)
	c := connectFake(t, f, cfg)
	f.mu.Lock()
	f.loseFinish = true
	f.mu.Unlock()
	if _, err := c.Upload(context.Background(), "a", strings.NewReader("abcd"), cloudstorage.UploadOptions{Resume: true}); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.finishCalls != 1 || f.preCalls != 1 {
		t.Fatalf("replayed non-idempotent calls: finish=%d pre=%d", f.finishCalls, f.preCalls)
	}
}

func TestCookieEnvironmentPersistenceAndDefaultDirectory(t *testing.T) {
	t.Chdir(t.TempDir())
	t.Setenv("QUARK_COOKIES", "__pus=bootstrap; __puus=old")
	t.Setenv("QUARK_COOKIE_FILE", "")
	f := newFake(t)
	cfg := Config{APIBaseURL: f.server.URL + "/api", AccountURL: f.server.URL + "/account", DisableAutoRefresh: true, HTTPClient: &http.Client{Transport: fakeTransport{f}}}
	p, err := New(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(DefaultCookieFile); err != nil {
		t.Fatal(err)
	}
	first := p.cookies.snapshot()
	p.Close()
	p, err = New(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if first == p.cookies.snapshot() {
		t.Fatal("reconnect did not refresh")
	}
	data, err := os.ReadFile(DefaultCookieFile)
	if err != nil {
		t.Fatal(err)
	}
	var record cookieRecord
	if err := json.Unmarshal(data, &record); err != nil {
		t.Fatal(err)
	}
	if record.Cookies != p.cookies.snapshot() {
		t.Fatal("refreshed cookie not persisted")
	}
	// Prove unchanged bootstrap input uses the newer file rather than old env values.
	var loaded cookieState
	if err := loaded.load(DefaultCookieFile, ""); err != nil {
		t.Fatal(err)
	}
	if loaded.snapshot() != record.Cookies {
		t.Fatal("stale environment rolled credentials back")
	}
}

// Drive the real HTTP request/response handlers without sandbox-prohibited TCP.
type fakeTransport struct{ f *fakeQuark }

func (t fakeTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	if err := r.Context().Err(); err != nil {
		return nil, err
	}
	w := &fakeResponse{ResponseRecorder: httptest.NewRecorder()}
	t.f.serve(w, r)
	if w.hijacked && w.Body.Len() == 0 {
		return nil, io.ErrUnexpectedEOF
	}
	return w.Result(), nil
}

type fakeResponse struct {
	*httptest.ResponseRecorder
	hijacked bool
}

func (w *fakeResponse) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	w.hijacked = true
	a, b := net.Pipe()
	b.Close()
	return a, nil, nil
}

func TestAutoRefreshStopsOnClose(t *testing.T) {
	f := newFake(t)
	cfg := f.cfg(t)
	cfg.DisableAutoRefresh = false
	cfg.RefreshInterval = 10 * time.Millisecond
	p, err := New(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.After(2 * time.Second)
	for {
		f.mu.Lock()
		count := f.refreshes
		f.mu.Unlock()
		if count > 1 {
			break
		}
		select {
		case <-deadline:
			t.Fatal("automatic refresh did not run")
		case <-time.After(5 * time.Millisecond):
		}
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-p.done:
	default:
		t.Fatal("refresh goroutine was not joined")
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
}
