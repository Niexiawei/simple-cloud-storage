package quark

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"time"

	cloudstorage "github.com/Niexiawei/simple-cloud-storage"
)

// UploadResumable uses confirmed local parts when the Web API provides no reliable
// part-list query. Every missing part is safely re-signed and re-uploaded by number.
// AuthInfo and callbacks live in TokenStore; checkpoint holds identifiers only.
func (p *Provider) UploadResumable(ctx context.Context, full string, source io.ReadSeeker, opts cloudstorage.UploadOptions, candidate cloudstorage.Checkpoint, store cloudstorage.CheckpointStore) (cloudstorage.ObjectInfo, error) {
	return p.uploadResumable(ctx, full, source, opts, candidate, store, true)
}

func (p *Provider) uploadResumable(ctx context.Context, full string, source io.ReadSeeker, opts cloudstorage.UploadOptions, candidate cloudstorage.Checkpoint, store cloudstorage.CheckpointStore, allowRestart bool) (cloudstorage.ObjectInfo, error) {
	if store == nil || opts.Size == nil {
		return cloudstorage.ObjectInfo{}, cloudstorage.ErrInvalidArgument
	}
	if err := candidate.Validate(); err != nil {
		return cloudstorage.ObjectInfo{}, err
	}
	if candidate.Path != full || candidate.AccountID != p.account || candidate.Provider != p.Name() {
		return cloudstorage.ObjectInfo{}, cloudstorage.ErrInvalidCheckpoint
	}
	current, err := store.Load(ctx, candidate.ID)
	var state cloudstorage.UploadState
	var session *uploadSession
	if err == nil {
		if current.ID != candidate.ID {
			return cloudstorage.ObjectInfo{}, cloudstorage.ErrInvalidCheckpoint
		}
		state, err = current.DecodeUpload()
		if err != nil {
			return cloudstorage.ObjectInfo{}, err
		}
		if err := state.MatchSource(*opts.Size, opts.SHA256); err != nil {
			return cloudstorage.ObjectInfo{}, err
		}
		candidate = current
		session, err = p.restore(ctx, full, state, opts)
		if err != nil {
			if !errors.Is(err, cloudstorage.ErrSessionExpired) {
				return cloudstorage.ObjectInfo{}, err
			}
			// Only an exact absent-FID response permits abandoning the old session.
			_, checkErr := p.fileInfo(ctx, state.Session["fid"])
			if !errors.Is(checkErr, cloudstorage.ErrNotFound) {
				return cloudstorage.ObjectInfo{}, err
			}
			if err := store.Delete(ctx, current.ID); err != nil {
				return cloudstorage.ObjectInfo{}, err
			}
			if err := ignoreNotFound(p.secrets.Delete(ctx, "quark-upload/"+p.account+"/"+state.Session["task_id"])); err != nil {
				return cloudstorage.ObjectInfo{}, err
			}
			session = nil
		}
	} else if !errors.Is(err, cloudstorage.ErrCheckpointNotFound) {
		return cloudstorage.ObjectInfo{}, err
	}
	if session == nil {
		session, err = p.create(ctx, full, opts)
		if err != nil {
			return cloudstorage.ObjectInfo{}, err
		}
		state, err = candidate.DecodeUpload()
		if err != nil {
			return cloudstorage.ObjectInfo{}, err
		}
		state.Completed = nil
		state.PartSize = session.PartSize()
		state.TotalParts = 0
		if state.Size > 0 {
			state.TotalParts = 1 + (state.Size-1)/state.PartSize
		}
		state.Session = map[string]string{"task_id": session.record.Pre.TaskID, "fid": session.record.Pre.FileID, "upload_id": session.record.Pre.UploadID, "obj_key": session.record.Pre.ObjectKey, "bucket": session.record.Pre.Bucket}
	}
	save := func() error {
		data, err := json.Marshal(state)
		if err != nil {
			return err
		}
		candidate.Data, candidate.UpdatedAt = data, time.Now().UTC()
		return store.Save(ctx, candidate)
	}
	if err := save(); err != nil {
		return cloudstorage.ObjectInfo{}, err
	}
	// Resolve ambiguous Commit/Finish before sending any additional body or PRE.
	if info, err := session.confirm(ctx); err == nil {
		return info, session.clearProgress(ctx, store, candidate.ID)
	} else if ctx.Err() != nil {
		return cloudstorage.ObjectInfo{}, ctx.Err()
	}
	if err := session.updateHash(ctx); err != nil {
		return cloudstorage.ObjectInfo{}, err
	}
	completed := make(map[int]cloudstorage.PartInfo, len(state.Completed))
	var transferred int64
	for _, part := range state.Completed {
		completed[part.PartNumber] = part
		transferred += part.Size
	}
	if opts.Progress != nil {
		opts.Progress(cloudstorage.Progress{Transferred: transferred, Total: state.Size})
	}
	if !session.instant && !session.record.Committed {
		buffer := make([]byte, int(state.PartSize))
		for number := int64(1); number <= state.TotalParts; number++ {
			if err := ctx.Err(); err != nil {
				return cloudstorage.ObjectInfo{}, err
			}
			if _, ok := completed[int(number)]; ok {
				continue
			}
			want := min(state.PartSize, state.Size-(number-1)*state.PartSize)
			if _, err := source.Seek((number-1)*state.PartSize, io.SeekStart); err != nil {
				return cloudstorage.ObjectInfo{}, err
			}
			if _, err := io.ReadFull(source, buffer[:want]); err != nil {
				return cloudstorage.ObjectInfo{}, errors.Join(cloudstorage.ErrSizeMismatch, err)
			}
			part, err := session.UploadPart(ctx, int(number), bytes.NewReader(buffer[:want]), want)
			if err != nil {
				if errors.Is(err, cloudstorage.ErrSessionExpired) {
					_, checkErr := p.fileInfo(ctx, session.record.Pre.FileID)
					if errors.Is(checkErr, cloudstorage.ErrNotFound) {
						if err := store.Delete(ctx, candidate.ID); err != nil {
							return cloudstorage.ObjectInfo{}, err
						}
						if err := ignoreNotFound(p.secrets.Delete(ctx, session.secretKey())); err != nil {
							return cloudstorage.ObjectInfo{}, err
						}
						if allowRestart {
							return p.uploadResumable(ctx, full, source, opts, candidate, store, false)
						}
						return cloudstorage.ObjectInfo{}, cloudstorage.ErrSessionExpired
					}
				}
				return cloudstorage.ObjectInfo{}, err
			}
			completed[part.PartNumber] = part
			state.Completed = append(state.Completed, part)
			if err := save(); err != nil {
				return cloudstorage.ObjectInfo{}, err
			}
			transferred += want
			if opts.Progress != nil {
				opts.Progress(cloudstorage.Progress{Transferred: transferred, Total: state.Size})
			}
		}
	}
	parts := make([]cloudstorage.PartInfo, 0, len(completed))
	for number := int64(1); number <= state.TotalParts; number++ {
		if part, ok := completed[int(number)]; ok {
			parts = append(parts, part)
		}
	}
	info, err := session.Complete(ctx, parts)
	if err != nil {
		return cloudstorage.ObjectInfo{}, err
	}
	if opts.Progress != nil {
		opts.Progress(cloudstorage.Progress{Transferred: state.Size, Total: state.Size})
	}
	return info, session.clearProgress(ctx, store, candidate.ID)
}

// AbortUpload abandons a retained session without deleting an already completed file.
func (p *Provider) AbortUpload(ctx context.Context, full string, identity cloudstorage.Checkpoint, store cloudstorage.CheckpointStore) error {
	cp, err := store.Load(ctx, identity.ID)
	if errors.Is(err, cloudstorage.ErrCheckpointNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	state, err := cp.DecodeUpload()
	if err != nil {
		return err
	}
	if cp.ID != identity.ID || cp.Path != full || cp.AccountID != p.account || cp.Provider != p.Name() {
		return cloudstorage.ErrInvalidCheckpoint
	}
	task := state.Session["task_id"]
	token, err := p.secrets.Load(ctx, "quark-upload/"+p.account+"/"+task)
	if errors.Is(err, cloudstorage.ErrTokenNotFound) {
		return store.Delete(ctx, cp.ID)
	}
	if err != nil {
		return err
	}
	var record sessionRecord
	if json.Unmarshal([]byte(token.Extra["session"]), &record) != nil || record.Path != full || record.Pre.TaskID != task {
		return cloudstorage.ErrInvalidCheckpoint
	}
	s := &uploadSession{p: p, record: record}
	if err := s.Abort(ctx); err != nil {
		return err
	}
	return ignoreNotFound(store.Delete(ctx, cp.ID))
}

func (p *Provider) restore(ctx context.Context, full string, state cloudstorage.UploadState, opts cloudstorage.UploadOptions) (*uploadSession, error) {
	task := state.Session["task_id"]
	if task == "" {
		return nil, cloudstorage.ErrInvalidCheckpoint
	}
	token, err := p.secrets.Load(ctx, "quark-upload/"+p.account+"/"+task)
	if errors.Is(err, cloudstorage.ErrTokenNotFound) {
		s := &uploadSession{p: p, record: sessionRecord{Pre: preData{TaskID: task, FileID: state.Session["fid"]}, Path: full, Size: state.Size, MD5: opts.MD5, SHA1: opts.SHA1, PartSize: state.PartSize}}
		if _, checkErr := s.confirm(ctx); checkErr == nil {
			s.done = true
			return s, nil
		}
		return nil, cloudstorage.ErrSessionExpired
	}
	if err != nil {
		return nil, err
	}
	var record sessionRecord
	if json.Unmarshal([]byte(token.Extra["session"]), &record) != nil {
		return nil, cloudstorage.ErrInvalidCheckpoint
	}
	if record.Path != full || record.Size != state.Size || record.PartSize != state.PartSize || record.MD5 != opts.MD5 || record.SHA1 != opts.SHA1 || record.Pre.TaskID != task || record.Pre.FileID != state.Session["fid"] || record.Pre.UploadID != state.Session["upload_id"] || record.Pre.ObjectKey != state.Session["obj_key"] || record.Pre.Bucket != state.Session["bucket"] {
		return nil, cloudstorage.ErrInvalidCheckpoint
	}
	if record.PartSize > 128<<20 {
		return nil, cloudstorage.ErrInvalidCheckpoint
	}
	if _, err := ossURL(record.Pre, p.allowLoopback); err != nil {
		return nil, cloudstorage.ErrInvalidCheckpoint
	}
	if record.Pre.AuthInfo == "" {
		return nil, cloudstorage.ErrSessionExpired
	}
	return &uploadSession{p: p, record: record}, nil
}

func (s *uploadSession) clearProgress(ctx context.Context, store cloudstorage.CheckpointStore, id string) error {
	if err := ignoreNotFound(s.p.secrets.Delete(ctx, s.secretKey())); err != nil {
		return err
	}
	if err := store.Delete(ctx, id); err != nil && !errors.Is(err, cloudstorage.ErrCheckpointNotFound) {
		return err
	}
	return nil
}
