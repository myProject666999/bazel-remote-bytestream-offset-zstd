package server_test

// Compressed-read offset unit and chunk boundary coverage.
//
// REAPI specifies that read_offset on compressed-blobs resources is an
// offset in the *uncompressed* form of the blob. The implementation indexes
// the casblob chunk table in those units and re-encodes the containing chunk
// when needed. These cases pin that down across all three on-disk formats:
//
//   - "zstd":   casblob header + zstd chunks (default storage mode)
//   - "legacy": headerless raw .v1 file (WithStorageMode("uncompressed"))
//   - "identity-header": casblob header with compression=Identity

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/buchgr/bazel-remote/v2/cache/disk"
	"github.com/buchgr/bazel-remote/v2/cache/disk/casblob"
	"github.com/buchgr/bazel-remote/v2/cache/disk/zstdimpl"
	testutils "github.com/buchgr/bazel-remote/v2/utils"
	"google.golang.org/grpc/codes"
)

type bndSizedBlob struct {
	size int64
	data []byte
	hash string
}

func bndChunkBlobs(t *testing.T) []bndSizedBlob {
	t.Helper()
	sizes := []int64{
		1,
		bnd15778ChunkSize - 1,
		bnd15778ChunkSize,
		bnd15778ChunkSize + 1,
		2 * bnd15778ChunkSize,
	}
	blobs := make([]bndSizedBlob, 0, len(sizes))
	for _, sz := range sizes {
		data, hash := bndRandomData(sz)
		blobs = append(blobs, bndSizedBlob{size: sz, data: data, hash: hash})
	}
	return blobs
}

// bndNewIdentityHeaderFixture writes casblob Identity-container files
// directly into the cache directory layout, then opens the disk cache and
// gRPC fixture on top.
func bndNewIdentityHeaderFixture(t *testing.T, blobs []bndSizedBlob) *bndFixture {
	t.Helper()

	dir := t.TempDir()
	zimpl, err := zstdimpl.Get("go")
	if err != nil {
		t.Fatalf("zstdimpl.Get: %v", err)
	}

	for _, b := range blobs {
		dirHash := filepath.Join(dir, "cas.v2", b.hash[:2])
		if err := os.MkdirAll(dirHash, 0o755); err != nil {
			t.Fatal(err)
		}
		p := filepath.Join(dirHash, fmt.Sprintf("%s-%d-bndseed", b.hash, b.size))
		f, err := os.OpenFile(p, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o644)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := casblob.WriteAndClose(zimpl, bytes.NewReader(b.data), f,
			casblob.Identity, b.hash, b.size); err != nil {
			t.Fatalf("WriteAndClose Identity %d: %v", b.size, err)
		}
	}

	dc, err := disk.New(dir, 1<<30,
		disk.WithAccessLogger(testutils.NewSilentLogger()))
	if err != nil {
		t.Fatalf("disk.New: %v", err)
	}

	return bndServeCache(t, dir, dc)
}

func bndOffsetCandidates(size int64) []int64 {
	cands := map[int64]bool{
		0: true, 1: true,
		bnd15778ChunkSize - 1: true,
		bnd15778ChunkSize:     true,
		bnd15778ChunkSize + 1: true,
		size - 1:              true,
		size:                  true,
		size + 1:              true,
	}
	out := make([]int64, 0, len(cands))
	for o := range cands {
		if o >= 0 && o <= size+1 {
			out = append(out, o)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func TestBnd15778_ChunkOffsetMatrix(t *testing.T) {
	blobs := bndChunkBlobs(t)

	zstdFx := bndNewFixture(t, "")
	legacyFx := bndNewFixture(t, "uncompressed")
	headerFx := bndNewIdentityHeaderFixture(t, blobs)

	for _, b := range blobs {
		zstdFx.put(t, b.hash, b.size, b.data)
		legacyFx.put(t, b.hash, b.size, b.data)
	}

	formats := []struct {
		name string
		fx   *bndFixture
	}{
		{"zstd-casblob", zstdFx},
		{"legacy-v1", legacyFx},
		{"identity-header", headerFx},
	}

	for _, format := range formats {
		for _, b := range blobs {
			for _, off := range bndOffsetCandidates(b.size) {
				for _, reqCompressed := range []bool{false, true} {
					formName := "blobs"
					rn := bndBlobResource(b.hash, b.size)
					if reqCompressed {
						formName = "compressed"
						rn = bndCompressedResource(b.hash, b.size)
					}

					t.Run(fmt.Sprintf("%s/size%d/off%d/%s",
						format.name, b.size, off, formName), func(t *testing.T) {

						out := bndRead(t, format.fx.bs, rn, off, 0)

						if off > b.size {
							if out.code != codes.OutOfRange {
								t.Fatalf("offset %d > size %d: expected OutOfRange, got %s (%s)",
									off, b.size, out.code, out.msg)
							}
							return
						}

						if out.code != codes.OK {
							t.Fatalf("offset %d: expected OK, got %s (%s)",
								off, out.code, out.msg)
						}

						got := out.data
						if reqCompressed {
							got = bndZstdDecode(t, out.data)
						}

						want := b.data[off:]
						if !bytes.Equal(got, want) {
							t.Fatalf("offset %d size %d: expected %d bytes, got %d; mismatch",
								off, b.size, len(want), len(got))
						}
					})
				}
			}
		}
	}
}

// Direct use of casblob.GetZstdReadCloser / GetUncompressedReadCloser with
// chunk aligned, boundary +/-1 and last-chunk offsets.
func TestBnd15778_CasblobDirectOffsetReaders(t *testing.T) {
	zimpl, err := zstdimpl.Get("go")
	if err != nil {
		t.Fatal(err)
	}

	const size = 2*bnd15778ChunkSize + 123
	data, hash := bndRandomData(size)

	path := filepath.Join(t.TempDir(), "blob.casblob")
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := casblob.WriteAndClose(zimpl, bytes.NewReader(data), f,
		casblob.Zstandard, hash, size); err != nil {
		t.Fatal(err)
	}

	offsets := []int64{
		0, 1,
		bnd15778ChunkSize - 1,
		bnd15778ChunkSize,
		bnd15778ChunkSize + 1,
		2*bnd15778ChunkSize - 1,
		2 * bnd15778ChunkSize,
		size - 1,
		size,
	}

	for _, off := range offsets {
		t.Run(fmt.Sprintf("uncompressed-reader/off%d", off), func(t *testing.T) {
			f, err := os.Open(path)
			if err != nil {
				t.Fatal(err)
			}
			rc, err := casblob.GetUncompressedReadCloser(zimpl, f, size, off)
			if err != nil {
				t.Fatalf("GetUncompressedReadCloser: %v", err)
			}
			got, err := io.ReadAll(rc)
			if err != nil {
				t.Fatalf("read all: %v", err)
			}
			_ = rc.Close()
			if !bytes.Equal(got, data[off:]) {
				t.Fatalf("uncompressed offset %d: want %d bytes got %d",
					off, len(data)-int(off), len(got))
			}
		})

		t.Run(fmt.Sprintf("zstd-reader/off%d", off), func(t *testing.T) {
			f, err := os.Open(path)
			if err != nil {
				t.Fatal(err)
			}
			rc, err := casblob.GetZstdReadCloser(zimpl, f, size, off)
			if err != nil {
				t.Fatalf("GetZstdReadCloser: %v", err)
			}
			raw, err := io.ReadAll(rc)
			if err != nil {
				t.Fatalf("read all: %v", err)
			}
			_ = rc.Close()
			got := bndZstdDecode(t, raw)
			if !bytes.Equal(got, data[off:]) {
				t.Fatalf("zstd offset %d: want %d bytes decoded got %d",
					off, len(data)-int(off), len(got))
			}
		})
	}
}
