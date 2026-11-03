// Package cloudstorage provides a prefix-scoped storage client and provider SPI.
//
// The quark and drivers packages provide the Quark Web driver, runtime credential
// configuration and a Close-managed refresh lifecycle. The common Client supports
// sequential multipart uploads, spooled Writers and checkpoint-based recovery for
// capable providers. Providers without recovery support reject Resume before I/O.
// A CheckpointStore alone does not enable resume. Abort abandons an operation;
// recoverable failures retain confirmed progress and complete spooled sources.
package cloudstorage
