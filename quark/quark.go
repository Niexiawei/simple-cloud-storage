// Package quark implements Quark's unofficial Web protocol without AList dependencies.
package quark

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	cloudstorage "github.com/Niexiawei/simple-cloud-storage"
)

const (
	DefaultCookieFile = "quark-cookies.json"
	defaultAPI        = "https://drive.quark.cn/1/clouddrive"
	apiUserAgent      = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36"
	userAgent         = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) quark-cloud-drive/2.5.20 Chrome/100.0.4896.160 Electron/18.3.5.4-b478491100 Safari/537.36 Channel/pckk_other_ch"
)

// Config is read at connection time. Cookies override QUARK_COOKIES/QUARK_COOKIE;
// a persisted refresh wins over an unchanged bootstrap cookie on the next start.
type Config struct {
	Cookies            string
	CookieFile         string // Defaults to QUARK_COOKIE_FILE, then cwd/quark-cookies.json.
	StateDir           string // Defaults to cwd/.simple-cloud-storage (checkpoints/session secrets).
	HTTPClient         *http.Client
	TokenStore         cloudstorage.TokenStore // Optional store for private upload-session credentials.
	RefreshInterval    time.Duration           // Zero selects 100 minutes. Negative is invalid.
	DisableAutoRefresh bool
	APIBaseURL         string // Optional API-compatible endpoint; HTTP is allowed only on loopback.
	AccountURL         string // Optional explicit account-info validation; unused by default.
}

// Provider owns Cookie refresh, directory IDs and private upload session credentials.
// Close it when no longer used to stop automatic refresh. Do not copy it.
type Provider struct {
	http                               *http.Client
	transfer                           *http.Client
	api, accountURL, account, stateDir string
	secrets                            cloudstorage.TokenStore
	cookies                            cookieState
	dirMu                              sync.Mutex
	dirs                               map[string]string
	refreshMu                          sync.Mutex
	refreshErr                         error
	cancel                             context.CancelFunc
	done                               chan struct{}
	closeOnce                          sync.Once
	closed                             atomic.Bool
	allowLoopback                      bool
	ownedTransport                     bool
}

var _ cloudstorage.Provider = (*Provider)(nil)
var _ cloudstorage.ResumableUploader = (*Provider)(nil)

// New initializes via the drive /config endpoint, refreshes its session Cookie
// and persists credentials. AccountURL opts into additional account validation.
// It never logs credentials. A failed initialization returns no usable provider.
func New(ctx context.Context, cfg Config) (*Provider, error) {
	if cfg.RefreshInterval < 0 {
		return nil, cloudstorage.ErrInvalidArgument
	}
	if cfg.RefreshInterval == 0 {
		cfg.RefreshInterval = 100 * time.Minute
	}
	if cfg.CookieFile == "" {
		cfg.CookieFile = os.Getenv("QUARK_COOKIE_FILE")
	}
	if cfg.CookieFile == "" {
		cfg.CookieFile = DefaultCookieFile
	}
	cookieFile, err := filepath.Abs(cfg.CookieFile)
	if err != nil {
		return nil, err
	}
	if cfg.StateDir == "" {
		cfg.StateDir = ".simple-cloud-storage"
	}
	stateDir, err := filepath.Abs(cfg.StateDir)
	if err != nil {
		return nil, err
	}
	if cfg.APIBaseURL == "" {
		cfg.APIBaseURL = defaultAPI
	}
	for _, endpoint := range []string{cfg.APIBaseURL, cfg.AccountURL} {
		if endpoint != "" && !validEndpoint(endpoint) {
			return nil, fmt.Errorf("quark endpoint: %w", cloudstorage.ErrInvalidArgument)
		}
	}
	client := http.Client{Timeout: 30 * time.Second}
	if cfg.HTTPClient != nil {
		client = *cfg.HTTPClient
	}
	if client.Timeout == 0 {
		client.Timeout = 30 * time.Second
	}
	// Cookie snapshots must not be amended by a shared Jar or forwarded on redirects.
	client.Jar = nil
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	dataClient := client
	if cfg.HTTPClient == nil || cfg.HTTPClient.Timeout == 0 {
		dataClient.Timeout = 0
	}
	if client.Transport == nil {
		if transport, ok := http.DefaultTransport.(*http.Transport); ok {
			transport = transport.Clone()
			transport.ResponseHeaderTimeout = 30 * time.Second
			client.Transport, dataClient.Transport = transport, transport
		}
	}
	p := &Provider{http: &client, transfer: &dataClient, api: cfg.APIBaseURL, accountURL: cfg.AccountURL, stateDir: stateDir, dirs: map[string]string{"": "0"}, done: make(chan struct{})}
	p.ownedTransport = (cfg.HTTPClient == nil || cfg.HTTPClient.Transport == nil) && client.Transport != nil
	apiURL, _ := url.Parse(cfg.APIBaseURL) // Endpoint was validated above.
	p.allowLoopback = isLoopback(apiURL.Hostname())
	if err := p.cookies.load(cookieFile, cfg.Cookies); err != nil {
		return nil, err
	}
	p.secrets = cfg.TokenStore
	if p.secrets == nil {
		p.secrets, err = cloudstorage.NewFileTokenStore(stateDir)
		if err != nil {
			return nil, err
		}
	}
	p.account, err = p.cookies.accountID()
	if err != nil {
		return nil, err
	}
	if p.accountURL != "" {
		p.account = ""
		var account struct {
			UID json.RawMessage `json:"uid"`
		}
		if err := p.request(ctx, http.MethodGet, p.accountURL, nil, nil, &account, "", false); err != nil {
			return nil, fmt.Errorf("quark account info: %w", err)
		}
		if err := json.Unmarshal(account.UID, &p.account); err != nil {
			var number json.Number
			if err := json.Unmarshal(account.UID, &number); err != nil {
				return nil, fmt.Errorf("quark account info uid: %w", cloudstorage.ErrAuthentication)
			}
			p.account = number.String()
		}
		if p.account == "" {
			return nil, fmt.Errorf("quark account info uid: %w", cloudstorage.ErrAuthentication)
		}
	}
	if err := p.RefreshCookies(ctx); err != nil {
		return nil, fmt.Errorf("quark refresh cookies: %w", err)
	}
	if err := p.cookies.activate(); err != nil {
		return nil, fmt.Errorf("quark persist cookies: %w", err)
	}
	if cfg.DisableAutoRefresh {
		close(p.done)
	} else {
		loopCtx, cancel := context.WithCancel(ctx)
		p.cancel = cancel
		go p.refreshLoop(loopCtx, cfg.RefreshInterval)
	}
	return p, nil
}

// Connection combines the common Client with the provider lifecycle.
type Connection struct {
	*cloudstorage.Client
	Provider *Provider
}

// Connect selects the Quark driver and supplies a default local CheckpointStore.
// Read credentials from Config or the environment on every new connection.
func Connect(ctx context.Context, cfg Config, storageCfg cloudstorage.Config) (*Connection, error) {
	p, err := New(ctx, cfg)
	if err != nil {
		return nil, err
	}
	if storageCfg.CheckpointStore == nil {
		storageCfg.CheckpointStore, err = cloudstorage.NewFileCheckpointStore(p.stateDir)
		if err != nil {
			p.Close()
			return nil, err
		}
	}
	c, err := cloudstorage.NewClient(p, storageCfg)
	if err != nil {
		p.Close()
		return nil, err
	}
	return &Connection{Client: c, Provider: p}, nil
}

// Close stops and joins automatic refresh. It is safe to call more than once.
func (p *Provider) Close() error {
	p.closeOnce.Do(func() {
		p.closed.Store(true)
		if p.cancel != nil {
			p.cancel()
		}
		<-p.done
		if p.ownedTransport {
			p.http.CloseIdleConnections()
		}
	})
	return nil
}

func (c *Connection) Close() error { return c.Provider.Close() }
func (p *Provider) Name() string   { return "quark" }
func (p *Provider) AccountID(ctx context.Context) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	return p.account, nil
}
func (p *Provider) Capabilities() cloudstorage.Capabilities {
	return cloudstorage.Capabilities{RequiresFullHash: true, RequiresMultipart: true, SupportsDownloadResume: true}
}

// RefreshCookies strips only the outgoing __puus so the service can reissue it.
// Failed refreshes leave the old in-memory and persisted credentials intact.
func (p *Provider) RefreshCookies(ctx context.Context) error {
	p.refreshMu.Lock()
	defer p.refreshMu.Unlock()
	snapshot := p.cookies.snapshot()
	err := p.request(ctx, http.MethodGet, p.api+"/config", nil, nil, nil, snapshot, true)
	p.refreshErr = err
	return err
}

// LastRefreshError reports background refresh/persistence failures without secrets.
func (p *Provider) LastRefreshError() error {
	p.refreshMu.Lock()
	defer p.refreshMu.Unlock()
	return p.refreshErr
}

func (p *Provider) refreshLoop(ctx context.Context, interval time.Duration) {
	defer close(p.done)
	timer := time.NewTimer(interval)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			err := p.RefreshCookies(ctx)
			if ctx.Err() != nil {
				return
			}
			delay := interval
			if err != nil {
				delay = min(delay, 5*time.Minute)
			}
			timer.Reset(delay)
		}
	}
}

func validEndpoint(raw string) bool {
	u, err := url.Parse(raw)
	return err == nil && u.Host != "" && u.User == nil && u.Fragment == "" && (u.Scheme == "https" || (u.Scheme == "http" && isLoopback(u.Hostname())))
}

func ignoreNotFound(err error) error {
	if errors.Is(err, cloudstorage.ErrTokenNotFound) || errors.Is(err, cloudstorage.ErrCheckpointNotFound) {
		return nil
	}
	return err
}
