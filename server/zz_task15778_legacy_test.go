package server_test

// Explicit coverage for legacy headerless .v1 CAS files: raw files produced
// by the "uncompressed" storage mode. Offsets are raw byte positions for
// blobs reads; compressed-blobs reads zstd-encode the suffix on the fly.

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"testing"

	"github.com/buchgr/bazel-remote/v2/cache"
	"google.golang.org/grpc/codes"
)

func TestBnd15778_LegacyV1FileOffsets(t *testing.T) {
	const size = int64(300)
	data, hash := bndRandomData(size)

	fx := bndNewFixture(t, "uncompressed")
	fx.put(t, hash, size, data)

	// The on-disk file must carry the .v1 legacy suffix.
	hits := bndCountHashFiles(t, fx.dir, hash)
	if len(hits) != 1 {
		t.Fatalf("expected exactly one legacy file for hash, got %v", hits)
	}
	if !bytes.HasSuffix([]byte(hits[0]), []byte(".v1")) {
		t.Fatalf("expected legacy .v1 file, got %q", hits[0])
	}

	// Direct disk cache Get offsets.
	for _, off := range []int64{0, 1, 128, 299, 300} {
		rc, found, err := fx.dc.Get(context.Background(), cache.CAS, hash, size, off)
		if err != nil {
			t.Fatalf("disk.Get off=%d: %v", off, err)
		}
		if found != size {
			t.Fatalf("off=%d foundSize=%d want %d", off, found, size)
		}
		got, err := io.ReadAll(rc)
		if err != nil {
			t.Fatal(err)
		}
		_ = rc.Close()
		if !bytes.Equal(got, data[off:]) {
			t.Fatalf("disk.Get off=%d: want %d bytes got %d", off, len(data)-int(off), len(got))
		}
	}

	// Offset past the end stays a bad request at the disk layer.
	if _, _, err := fx.dc.Get(context.Background(), cache.CAS, hash, size, 301); err == nil {
		t.Fatal("expected bad-request error for offset > size at disk layer")
	}

	// Through gRPC: uncompressed and compressed resource names share the
	// same uncompressed offset unit.
	for _, off := range []int64{0, 128, 299, 300} {
		out := bndRead(t, fx.bs, bndBlobResource(hash, size), off, 0)
		if out.code != codes.OK || !bytes.Equal(out.data, data[off:]) {
			t.Fatalf("blobs off=%d code=%s len=%d", off, out.code, len(out.data))
		}

		zout := bndRead(t, fx.bs, bndCompressedResource(hash, size), off, 0)
		if zout.code != codes.OK {
			t.Fatalf("compressed off=%d code=%s (%s)", off, zout.code, zout.msg)
		}
		if got := bndZstdDecode(t, zout.data); !bytes.Equal(got, data[off:]) {
			t.Fatalf("compressed legacy off=%d: decoded %d bytes want %d",
				off, len(got), len(data)-int(off))
		}
	}

	// Out of range through gRPC.
	out := bndRead(t, fx.bs, fmt.Sprintf("bnd/blobs/%s/%d", hash, size), size+1, 0)
	if out.code != codes.OutOfRange {
		t.Fatalf("expected OutOfRange, got %s", out.code)
	}
}
