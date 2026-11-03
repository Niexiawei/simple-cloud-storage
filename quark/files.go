package quark

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
	"time"

	cloudstorage "github.com/Niexiawei/simple-cloud-storage"
	"github.com/Niexiawei/simple-cloud-storage/internal/pathx"
)

type file struct {
	ID        string `json:"fid"`
	Name      string `json:"file_name"`
	IsFile    bool   `json:"file"`
	Size      int64  `json:"size"`
	MD5       string `json:"md5"`
	SHA1      string `json:"sha1"`
	UpdatedAt int64  `json:"updated_at"`
	Status    int    `json:"status"`
}

func (f file) info() cloudstorage.ObjectInfo {
	info := cloudstorage.ObjectInfo{ID: f.ID, Size: f.Size, MD5: f.MD5, SHA1: f.SHA1}
	if f.UpdatedAt > 0 {
		info.ModTime = time.UnixMilli(f.UpdatedAt)
	}
	return info
}

func (p *Provider) find(ctx context.Context, parent, name string) (file, error) {
	var found *file
	for page := 1; page <= 1000; page++ {
		var result envelope
		query := url.Values{"pdir_fid": {parent}, "_page": {strconv.Itoa(page)}, "_size": {"100"}, "_fetch_total": {"1"}, "fetch_all_file": {"1"}, "fetch_risk_file_name": {"1"}}
		if err := p.call(ctx, http.MethodGet, "/file/sort", query, nil, &result); err != nil {
			return file{}, err
		}
		var data struct {
			List *[]file `json:"list"`
		}
		var meta struct {
			Total *int `json:"_total"`
		}
		if json.Unmarshal(result.Data, &data) != nil || data.List == nil {
			return file{}, fmt.Errorf("quark list: missing or invalid data.list: %w", cloudstorage.ErrProtocol)
		}
		if len(result.Metadata) > 0 && json.Unmarshal(result.Metadata, &meta) != nil {
			return file{}, fmt.Errorf("quark list: invalid metadata: %w", cloudstorage.ErrProtocol)
		}
		if meta.Total != nil && (*meta.Total < 0 || *meta.Total < (page-1)*100+len(*data.List)) {
			return file{}, fmt.Errorf("quark list: inconsistent total=%d page=%d count=%d: %w", *meta.Total, page, len(*data.List), cloudstorage.ErrProtocol)
		}
		for _, f := range *data.List {
			if html.UnescapeString(f.Name) == name {
				if f.ID == "" {
					return file{}, fmt.Errorf("quark list: missing fid: %w", cloudstorage.ErrProtocol)
				}
				if found != nil {
					return file{}, cloudstorage.ErrConflict
				}
				copy := f
				found = &copy
			}
		}
		if (meta.Total != nil && page*100 >= *meta.Total) || (meta.Total == nil && len(*data.List) < 100) {
			if found == nil {
				return file{}, cloudstorage.ErrNotFound
			}
			return *found, nil
		}
	}
	return file{}, fmt.Errorf("quark list: pagination limit exceeded: %w", cloudstorage.ErrProtocol)
}

func (p *Provider) directory(ctx context.Context, dir string, create bool) (string, error) {
	if dir == "." {
		dir = ""
	}
	if dir != "" {
		if _, err := pathx.Key(dir); err != nil {
			return "", cloudstorage.ErrInvalidArgument
		}
	}
	// Only IDs are cached; URLs and signatures are fetched anew for every transfer.
	p.dirMu.Lock()
	defer p.dirMu.Unlock()
	if err := ctx.Err(); err != nil {
		return "", err
	}
	parent := "0"
	var prefix string
	for _, name := range strings.Split(dir, "/") {
		if name == "" {
			continue
		}
		prefix = path.Join(prefix, name)
		if cached, ok := p.dirs[prefix]; ok {
			parent = cached
			continue
		}
		f, err := p.find(ctx, parent, name)
		if errors.Is(err, cloudstorage.ErrNotFound) && create {
			createErr := p.call(ctx, http.MethodPost, "/file", nil, map[string]any{"dir_init_lock": false, "dir_path": "", "file_name": name, "pdir_fid": parent}, nil)
			if createErr != nil && !errors.Is(createErr, cloudstorage.ErrConflict) {
				return "", createErr
			}
			for attempt := 0; attempt < 5; attempt++ {
				f, err = p.find(ctx, parent, name)
				if err == nil || !errors.Is(err, cloudstorage.ErrNotFound) {
					break
				}
				if err := wait(ctx, time.Duration(attempt+1)*100*time.Millisecond); err != nil {
					return "", err
				}
			}
		}
		if err != nil {
			return "", err
		}
		if f.IsFile {
			return "", cloudstorage.ErrConflict
		}
		p.dirs[prefix] = f.ID
		parent = f.ID
	}
	return parent, nil
}

func (p *Provider) resolveFile(ctx context.Context, full string) (file, error) {
	if _, err := pathx.Key(full); err != nil {
		return file{}, cloudstorage.ErrInvalidArgument
	}
	parent, err := p.directory(ctx, path.Dir(full), false)
	if err != nil {
		return file{}, err
	}
	f, err := p.find(ctx, parent, path.Base(full))
	if err != nil {
		return file{}, err
	}
	if !f.IsFile {
		return file{}, cloudstorage.ErrConflict
	}
	current, err := p.fileInfo(ctx, f.ID)
	if err != nil {
		return file{}, err
	}
	if html.UnescapeString(current.Name) != path.Base(full) {
		return file{}, cloudstorage.ErrConflict
	}
	return current, nil
}

func (p *Provider) fileInfo(ctx context.Context, id string) (file, error) {
	var f file
	if err := p.call(ctx, http.MethodGet, "/file/info", url.Values{"fid": {id}}, nil, &f); err != nil {
		return f, err
	}
	if f.ID != id || !f.IsFile || f.Size < 0 {
		return file{}, fmt.Errorf("quark file info: fid_match=%t file=%t size=%d name_present=%t: %w", f.ID == id, f.IsFile, f.Size, f.Name != "", cloudstorage.ErrProtocol)
	}
	return f, nil
}

func (p *Provider) Stat(ctx context.Context, full string) (cloudstorage.ObjectInfo, error) {
	f, err := p.resolveFile(ctx, full)
	return f.info(), err
}

func (p *Provider) Delete(ctx context.Context, full string) error {
	f, err := p.resolveFile(ctx, full)
	if err != nil {
		return err
	}
	err = p.call(ctx, http.MethodPost, "/file/delete", nil, map[string]any{"action_type": 1, "exclude_fids": []string{}, "filelist": []string{f.ID}}, nil)
	// A lost response is resolved by FID rather than replaying a destructive call.
	for attempt := 0; attempt < 5; attempt++ {
		_, checkErr := p.fileInfo(ctx, f.ID)
		if errors.Is(checkErr, cloudstorage.ErrNotFound) {
			return nil
		}
		if checkErr != nil {
			return errors.Join(err, checkErr)
		}
		if err != nil {
			return err
		}
		if err := wait(ctx, 200*time.Millisecond); err != nil {
			return err
		}
	}
	return cloudstorage.ErrTemporary
}
