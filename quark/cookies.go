package quark

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	cloudstorage "github.com/Niexiawei/simple-cloud-storage"
)

type cookieRecord struct {
	Cookies   string    `json:"cookies"`
	SeedHash  string    `json:"seed_hash"`
	UpdatedAt time.Time `json:"updated_at"`
}
type cookieState struct {
	mu     sync.Mutex
	record cookieRecord
	path   string
	ready  bool
}

func (c *cookieState) load(path, explicit string) error {
	c.path = path
	data, err := os.ReadFile(path)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("read quark cookie file: %w", err)
	}
	var stored cookieRecord
	if err == nil {
		if json.Unmarshal(data, &stored) != nil {
			return fmt.Errorf("quark cookie file is invalid: %w", cloudstorage.ErrAuthentication)
		}
	}
	seed := explicit
	if seed == "" {
		seed = os.Getenv("QUARK_COOKIES")
	}
	if seed == "" {
		seed = os.Getenv("QUARK_COOKIE")
	}
	if seed != "" {
		parsed, err := parseCookies(seed)
		if err != nil {
			return err
		}
		seed = formatCookies(parsed)
		sum := sha256.Sum256([]byte(seed))
		hash := hex.EncodeToString(sum[:])
		if stored.Cookies != "" && stored.SeedHash == hash {
			c.record = stored
		} else {
			c.record = cookieRecord{Cookies: seed, SeedHash: hash}
		}
	} else {
		c.record = stored
	}
	parsed, err := parseCookies(c.record.Cookies)
	if err != nil {
		return err
	}
	c.record.Cookies = formatCookies(parsed)
	// Older or manually provisioned files may not have a bootstrap fingerprint.
	// Establish it once before refresh; persist it alongside refreshed credentials.
	if c.record.SeedHash == "" {
		sum := sha256.Sum256([]byte(c.record.Cookies))
		c.record.SeedHash = hex.EncodeToString(sum[:])
	}
	return nil
}

func (c *cookieState) accountID() (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	values, err := parseCookies(c.record.Cookies)
	if err != nil {
		return "", err
	}
	if uid := values["__uid"]; uid != "" {
		return uid, nil
	}
	// Cookies without __uid still work with /config in the reference driver.
	// A stable opaque namespace isolates their checkpoints without depending on
	// an unrelated account endpoint or on the rotating __puus cookie.
	sum := sha256.Sum256([]byte("quark account\x00" + c.record.SeedHash))
	return "cookie-" + hex.EncodeToString(sum[:]), nil
}

func parseCookies(value string) (map[string]string, error) {
	if strings.TrimSpace(value) == "" || strings.ContainsAny(value, "\r\n\x00") {
		return nil, fmt.Errorf("quark cookies are missing or invalid: %w", cloudstorage.ErrAuthentication)
	}
	parsed := make(map[string]string)
	for _, part := range strings.Split(value, ";") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		name, value, ok := strings.Cut(part, "=")
		cookie := http.Cookie{Name: strings.TrimSpace(name), Value: strings.TrimSpace(value)}
		if !ok || cookie.Valid() != nil {
			return nil, fmt.Errorf("quark cookie syntax: %w", cloudstorage.ErrInvalidArgument)
		}
		parsed[cookie.Name] = cookie.Value
	}
	if len(parsed) == 0 {
		return nil, cloudstorage.ErrAuthentication
	}
	return parsed, nil
}

func formatCookies(values map[string]string) string {
	names := make([]string, 0, len(values))
	for name := range values {
		names = append(names, name)
	}
	sort.Strings(names)
	parts := make([]string, 0, len(names))
	for _, name := range names {
		parts = append(parts, name+"="+values[name])
	}
	return strings.Join(parts, "; ")
}

func (c *cookieState) snapshot() string { c.mu.Lock(); defer c.mu.Unlock(); return c.record.Cookies }

func (c *cookieState) activate() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.saveLocked(c.record); err != nil {
		return err
	}
	c.ready = true
	return nil
}

func (c *cookieState) merge(cookies []*http.Cookie, snapshot string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	current, err := parseCookies(c.record.Cookies)
	if err != nil {
		return err
	}
	before, err := parseCookies(snapshot)
	// A refresh with all cookies stripped can have an empty snapshot.
	if err != nil {
		before = map[string]string{}
	}
	changed := false
	for _, cookie := range cookies {
		// Only authentication cookies known to the Quark protocol are persisted.
		switch cookie.Name {
		case "__puus", "__pus", "__kps", "__ktd":
		default:
			continue
		}
		if cookie.Valid() != nil {
			return cloudstorage.ErrProtocol
		}
		if current[cookie.Name] != before[cookie.Name] {
			continue
		}
		if cookie.MaxAge < 0 || (!cookie.Expires.IsZero() && cookie.Expires.Before(time.Now())) {
			delete(current, cookie.Name)
			changed = true
			continue
		}
		if current[cookie.Name] != cookie.Value {
			current[cookie.Name] = cookie.Value
			changed = true
		}
	}
	if !changed {
		return nil
	}
	next := c.record
	next.Cookies = formatCookies(current)
	// Serialize disk replacement with updates so an older response cannot overwrite
	// a newer persisted snapshot. No HTTP call is made while holding this lock.
	if c.ready {
		if err := c.saveLocked(next); err != nil {
			return err
		}
	}
	c.record = next
	return nil
}

func (c *cookieState) saveLocked(record cookieRecord) error {
	record.UpdatedAt = time.Now().UTC()
	data, err := json.Marshal(record)
	if err != nil {
		return err
	}
	dir := filepath.Dir(c.path)
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	f, err := os.CreateTemp(dir, ".quark-cookie-*.tmp")
	if err != nil {
		return err
	}
	name := f.Name()
	defer func() { f.Close(); os.Remove(name) }()
	if err := f.Chmod(0600); err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(name, c.path); err != nil {
		return fmt.Errorf("persist quark cookies: %w", err)
	}
	return nil
}
