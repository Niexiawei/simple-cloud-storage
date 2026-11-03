package quark

import (
	"context"
	"os"
	"testing"
	"time"
)

// Opt in explicitly. This validates login/refresh and credential persistence only;
// it never creates directories, uploads, deletes, or prints cookies.
func TestLiveAccountReadOnly(t *testing.T) {
	if os.Getenv("QUARK_LIVE_TEST") != "1" {
		t.Skip("set QUARK_LIVE_TEST=1 and configure local cookies to verify a real account")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	p, err := New(ctx, Config{DisableAutoRefresh: true})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if account, err := p.AccountID(ctx); err != nil || account == "" {
		t.Fatal("account validation failed")
	}
}
