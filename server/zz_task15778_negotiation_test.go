package server_test

// Compression negotiation and resource name parsing boundaries:
//   - the compressor set statically advertised in Capabilities must equal the
//     set accepted by parseReadResource/parseWriteResource (observed through
//     the gRPC API here). A future compressor added to only one side fails
//     this test;
//   - only the "zstd" token is accepted;
//   - write resource names allow extra trailing metadata segments, read
//     resource names do not;
//   - an instance name containing "blobs" as a distinct path segment is
//     ambiguous and rejected;
//   - switching physical storage mode (zstd casblob vs uncompressed legacy)
//     does not change any compressed-blobs Read result.

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/google/uuid"
	"google.golang.org/genproto/googleapis/bytestream"
	"google.golang.org/grpc/codes"

	pb "github.com/buchgr/bazel-remote/v2/genproto/build/bazel/remote/execution/v2"
)

// bndParseReadAccept reports whether a compressed-blobs read resource with
// the given compressor token gets past resource parsing. Parse failures are
// InvalidArgument; a parsed-but-missing blob is NotFound.
func bndParseReadAccept(t *testing.T, fx *bndFixture, token string) bool {
	t.Helper()
	rn := fmt.Sprintf("bnd-inst/compressed-blobs/%s/%s/42",
		token, strings.Repeat("a", 64))
	out := bndRead(t, fx.bs, rn, 0, 0)
	switch out.code {
	case codes.InvalidArgument:
		if strings.Contains(out.msg, "parse resource name") ||
			strings.Contains(out.msg, "compressor") {
			return false
		}
		// Hash validation failures also mean the compressor parsed; our hash
		// is syntactically valid so this should not happen.
		return true
	case codes.NotFound:
		return true
	default:
		t.Fatalf("unexpected code %s for token %q: %s", out.code, token, out.msg)
		return false
	}
}

// bndParseWriteAccept classifies parseWriteResource via QueryWriteStatus,
// which parses the same resource name without starting a stream.
func bndParseWriteAccept(t *testing.T, fx *bndFixture, rn string) codes.Code {
	t.Helper()
	_, err := fx.bs.QueryWriteStatus(context.Background(),
		&bytestream.QueryWriteStatusRequest{ResourceName: rn})
	return bndStatusCode(err)
}

func TestBnd15778_CompressorSetsMatchCapabilities(t *testing.T) {
	fx := bndNewFixture(t, "")

	caps, err := fx.caps.GetCapabilities(context.Background(),
		&pb.GetCapabilitiesRequest{})
	if err != nil {
		t.Fatal(err)
	}
	cc := caps.GetCacheCapabilities()
	if cc == nil {
		t.Fatal("nil cache capabilities")
	}

	advertised := map[string]bool{}
	for _, c := range cc.SupportedCompressors {
		advertised[strings.ToLower(pb.Compressor_Value_name[int32(c)])] = true
	}
	batchAdvertised := map[string]bool{}
	for _, c := range cc.SupportedBatchUpdateCompressors {
		batchAdvertised[strings.ToLower(pb.Compressor_Value_name[int32(c)])] = true
	}

	// Both capability lists are statically pinned to ZSTD.
	if len(cc.SupportedCompressors) != 1 ||
		cc.SupportedCompressors[0] != pb.Compressor_ZSTD {
		t.Fatalf("SupportedCompressors must be exactly [ZSTD], got %v",
			cc.SupportedCompressors)
	}
	if len(cc.SupportedBatchUpdateCompressors) != 1 ||
		cc.SupportedBatchUpdateCompressors[0] != pb.Compressor_ZSTD {
		t.Fatalf("SupportedBatchUpdateCompressors must be exactly [ZSTD], got %v",
			cc.SupportedBatchUpdateCompressors)
	}

	// For every compressor known to the REAPI genproto, parser acceptance
	// must equal capability advertisement. If a future compressor is added
	// to exactly one of grpc.go's capabilities or the two resource parsers,
	// one of these comparisons fails.
	for name, num := range pb.Compressor_Value_value {
		token := strings.ToLower(name)
		if pb.Compressor_Value(num) == pb.Compressor_IDENTITY {
			// Identity is mandatory and implicit; compressed-blobs/identity
			// is still rejected by the parsers.
			if bndParseReadAccept(t, fx, token) {
				t.Fatalf("compressed-blobs/%s must be rejected", token)
			}
			continue
		}

		_, advertisedCap := advertised[token]
		readAccepted := bndParseReadAccept(t, fx, token)

		writeRN := fmt.Sprintf("uploads/%s/compressed-blobs/%s/%s/42",
			uuid.NewString(), token, strings.Repeat("a", 64))
		writeCode := bndParseWriteAccept(t, fx, writeRN)
		writeAccepted := writeCode != codes.InvalidArgument

		if readAccepted != advertisedCap {
			t.Fatalf("compressor %q: read parser accepted=%v but capabilities advertised=%v",
				token, readAccepted, advertisedCap)
		}
		if writeAccepted != advertisedCap {
			t.Fatalf("compressor %q: write parser accepted=%v (code %s) but capabilities advertised=%v",
				token, writeAccepted, writeCode, advertisedCap)
		}
	}

	// Explicitly pin the only accepted literal.
	if !bndParseReadAccept(t, fx, "zstd") {
		t.Fatal("zstd must be accepted by read parser")
	}
	for _, bad := range []string{"zstandard", "Zstd", "ZSTD", "gzip", "none", ""} {
		if bad == "" {
			continue // empty token changes segment count, covered elsewhere
		}
		if bndParseReadAccept(t, fx, bad) {
			t.Fatalf("compressor token %q must be rejected", bad)
		}
	}
}

func TestBnd15778_ResourceNameMetadataAndAmbiguity(t *testing.T) {
	fx := bndNewFixture(t, "")
	data, hash := bndRandomData(3)
	fx.put(t, hash, 3, data)

	// Write names allow arbitrary extra path segments after the size.
	writeRN := fmt.Sprintf("uploads/%s/blobs/%s/3/optional/metadata/segments",
		uuid.NewString(), hash)
	if _, err := bndSendWrite(t, fx, writeRN, data, []int{1, 2}, 0); err != nil {
		t.Fatalf("write with metadata segments: %v", err)
	}

	// QueryWriteStatus parses the same write name with metadata suffix.
	if code := bndParseWriteAccept(t, fx, writeRN); code != codes.OK {
		t.Fatalf("QWS on write name with metadata: expected OK, got %s", code)
	}

	// Compressed write names accept trailing metadata segments too.
	compressedMetaRN := fmt.Sprintf(
		"uploads/%s/compressed-blobs/zstd/%s/3/meta/after/size",
		uuid.NewString(), hash)
	if code := bndParseWriteAccept(t, fx, compressedMetaRN); code != codes.OK {
		t.Fatalf("QWS on compressed write name with metadata: expected OK, got %s", code)
	}

	// Read names do NOT allow extra segments.
	for _, rn := range []string{
		fmt.Sprintf("blobs/%s/3/metadata", hash),
		fmt.Sprintf("compressed-blobs/zstd/%s/3/metadata", hash),
	} {
		if out := bndRead(t, fx.bs, rn, 0, 0); out.code != codes.InvalidArgument {
			t.Fatalf("read %q with trailing segment: expected InvalidArgument, got %s",
				rn, out.code)
		}
	}

	// An instance name containing "blobs" as a distinct path segment makes
	// the resource name unparseable; REAPI forbids such instance segments.
	badReadNames := []string{
		fmt.Sprintf("blobs/blobs/%s/3", hash),
		fmt.Sprintf("blobs/foo/blobs/%s/3", hash),
		fmt.Sprintf("foo/blobs/blobs/%s/3", hash),
	}
	for _, rn := range badReadNames {
		if out := bndRead(t, fx.bs, rn, 0, 0); out.code != codes.InvalidArgument {
			t.Fatalf("ambiguous name %q: expected InvalidArgument, got %s (%s)",
				rn, out.code, out.msg)
		}
	}

	// A multi-segment instance name without the forbidden keyword parses and
	// reaches the blob (OK here because the blob exists).
	rn := fmt.Sprintf("org/instance/name/blobs/%s/3", hash)
	if out := bndRead(t, fx.bs, rn, 0, 0); out.code != codes.OK {
		t.Fatalf("multi-segment instance name: expected OK, got %s (%s)",
			out.code, out.msg)
	}
}

func TestBnd15778_StorageModeSwitchEquivalence(t *testing.T) {
	blobs := bndChunkBlobs(t)

	zstdFx := bndNewFixture(t, "")
	legacyFx := bndNewFixture(t, "uncompressed")

	for _, b := range blobs {
		zstdFx.put(t, b.hash, b.size, b.data)
		legacyFx.put(t, b.hash, b.size, b.data)
	}

	offsets := []int64{
		0, 1, bnd15778ChunkSize, bnd15778ChunkSize + 1,
	}

	for _, b := range blobs {
		for _, off := range offsets {
			if off > b.size {
				continue
			}
			for _, compressedName := range []bool{false, true} {
				rn := bndBlobResource(b.hash, b.size)
				if compressedName {
					rn = bndCompressedResource(b.hash, b.size)
				}

				a := bndRead(t, zstdFx.bs, rn, off, 0)
				c := bndRead(t, legacyFx.bs, rn, off, 0)

				name := fmt.Sprintf("size%d/off%d/compressedName%v",
					b.size, off, compressedName)
				t.Run(name, func(t *testing.T) {
					if a.code != c.code {
						t.Fatalf("status differs across storage modes: %s vs %s",
							a.code, c.code)
					}
					ga, gc := a.data, c.data
					if compressedName {
						ga = bndZstdDecode(t, a.data)
						gc = bndZstdDecode(t, c.data)
					}
					if !bytes.Equal(ga, gc) {
						t.Fatalf("result differs across storage modes at off %d", off)
					}
					if !bytes.Equal(ga, b.data[off:]) {
						t.Fatalf("unexpected content at off %d", off)
					}
				})
			}
		}
	}
}
