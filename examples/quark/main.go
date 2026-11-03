// Quark example: build once, supply credentials and operation flags at runtime.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"

	cloudstorage "github.com/Niexiawei/simple-cloud-storage"
	"github.com/Niexiawei/simple-cloud-storage/drivers"
	"github.com/Niexiawei/simple-cloud-storage/quark"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	provider := flag.String("provider", os.Getenv("CLOUD_STORAGE_PROVIDER"), "driver name (quark)")
	prefix := flag.String("prefix", os.Getenv("QUARK_PREFIX"), "required cloud directory prefix")
	key := flag.String("key", "", "logical object key relative to prefix")
	upload := flag.String("upload", "", "upload a local file")
	download := flag.String("download", "", "download to a local file")
	cookieFile := flag.String("cookie-file", "", "cookie persistence path; default cwd/quark-cookies.json")
	resume := flag.Bool("resume", true, "persist transfer progress for recovery")
	flag.Parse()
	if *prefix == "" || *key == "" || (*upload != "" && *download != "") {
		return errors.New("provide -prefix and -key; choose at most one of -upload or -download")
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()
	client, err := drivers.Connect(ctx, drivers.Config{Provider: *provider, Storage: cloudstorage.Config{Prefix: *prefix}, Quark: quark.Config{CookieFile: *cookieFile}})
	if err != nil {
		return err
	}
	defer client.Close()
	switch {
	case *upload != "":
		info, err := client.UploadFile(ctx, *key, *upload, cloudstorage.UploadOptions{Resume: *resume})
		if err != nil {
			return err
		}
		fmt.Printf("uploaded key=%s size=%d\n", info.Key, info.Size)
	case *download != "":
		return client.DownloadToFile(ctx, *key, *download, cloudstorage.DownloadOptions{Resume: *resume})
	default:
		info, err := client.Stat(ctx, *key)
		if err != nil {
			return err
		}
		fmt.Printf("key=%s size=%d\n", info.Key, info.Size)
	}
	return nil
}
