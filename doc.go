// Package cloudstorage provides a prefix-scoped storage client and provider SPI.
//
// Phase 1 supports sequential multipart uploads and bounded-memory Writers with
// temporary-file spooling for unknown-size or full-hash inputs. Real cloud
// providers and cross-process transfer recovery are not implemented yet.
// Resume requests return ErrInvalidArgument for missing configuration, otherwise
// ErrUnsupported, before transfer I/O. Merely setting a CheckpointStore does not
// enable resume. Abort abandons an operation; it is not a resumable pause.
package cloudstorage
