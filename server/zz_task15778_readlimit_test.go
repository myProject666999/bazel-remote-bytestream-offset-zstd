package server_test

// ReadLimit boundary behavior. ByteStream defines read_limit as the maximum
// number of data bytes the server may return: it is a cap, so a limit smaller
// than the blob suffix yields exactly that many bytes and codes.OK, not
// OutOfRange. Negative limits remain OutOfRange for uncompressed resources;
// REAPI requires InvalidArgument for any non-zero limit on compressed-blobs.

import (
	"bytes"
	"fmt"
	"testing"

	"google.golang.org/grpc/codes"
)

func TestBnd15778_ReadLimitUncompressed(t *testing.T) {
	const size = int64(10)
	data, hash := bndRandomData(size)
	fx := bndNewFixture(t, "")
	fx.put(t, hash, size, data)
	rn := bndBlobResource(hash, size)

	rows := []struct {
		name      string
		offset    int64
		limit     int64
		wantCode  codes.Code
		wantBytes int // expected returned byte count on OK
	}{
		{"limit-zero-means-all", 0, 0, codes.OK, 10},
		{"limit-one-truncates", 0, 1, codes.OK, 1},
		{"limit-three-truncates", 0, 3, codes.OK, 3},
		{"limit-equal-size", 0, 10, codes.OK, 10},
		{"limit-larger-than-size", 0, 11, codes.OK, 10},
		{"offset9-limit1", 9, 1, codes.OK, 1},
		{"offset8-limit5-eof-first", 8, 5, codes.OK, 2},
		{"negative-limit-out-of-range", 0, -1, codes.OutOfRange, 0},
	}

	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			out := bndRead(t, fx.bs, rn, row.offset, row.limit)
			if out.code != row.wantCode {
				t.Fatalf("offset=%d limit=%d: expected %s, got %s (%s)",
					row.offset, row.limit, row.wantCode, out.code, out.msg)
			}
			if row.wantCode != codes.OK {
				return
			}
			if len(out.data) != row.wantBytes {
				t.Fatalf("expected %d bytes, got %d", row.wantBytes, len(out.data))
			}
			want := data[row.offset : row.offset+int64(row.wantBytes)]
			if !bytes.Equal(out.data, want) {
				t.Fatalf("content mismatch\nwant %v\ngot  %v", want, out.data)
			}
		})
	}
}

// A multi-megabyte blob exercises the chunked send path with a limit that
// lands strictly between read chunks.
func TestBnd15778_ReadLimitLargeBlobTruncatesMidStream(t *testing.T) {
	size := int64(2)*1024*1024 + 123
	data, hash := bndRandomData(size)
	fx := bndNewFixture(t, "")
	fx.put(t, hash, size, data)

	const offset = int64(777)
	const limit = int64(1500003)

	out := bndRead(t, fx.bs, bndBlobResource(hash, size), offset, limit)
	if out.code != codes.OK {
		t.Fatalf("expected OK truncation, got %s (%s)", out.code, out.msg)
	}
	if int64(len(out.data)) != limit {
		t.Fatalf("expected exactly read_limit=%d bytes, got %d", limit, len(out.data))
	}
	if !bytes.Equal(out.data, data[offset:offset+limit]) {
		t.Fatalf("truncated content mismatch")
	}
}

func TestBnd15778_ReadLimitCompressedRejected(t *testing.T) {
	const size = int64(10)
	data, hash := bndRandomData(size)
	fx := bndNewFixture(t, "")
	fx.put(t, hash, size, data)
	rn := bndCompressedResource(hash, size)

	for _, limit := range []int64{1, 1024, -1} {
		t.Run(fmt.Sprintf("limit-%d", limit), func(t *testing.T) {
			out := bndRead(t, fx.bs, rn, 0, limit)
			if out.code != codes.InvalidArgument {
				t.Fatalf("limit %d: expected InvalidArgument, got %s (%s)",
					limit, out.code, out.msg)
			}
		})
	}
}
