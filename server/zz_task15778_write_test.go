package server_test

// Write-side boundaries:
//   - non-zero WriteOffset on a missing blob is rejected (no resume support),
//     while an already-existing blob short-circuits successfully;
//   - QueryWriteStatus only ever reports {0,false} or {size,true};
//   - uncompressed oversize is OutOfRange as soon as the declared size is
//     crossed;
//   - compressed uploads are size checked after decompression (Internal via
//     the disk cache today), for both longer and shorter payloads, and never
//     leave temp files behind;
//   - controlled chunking must not change any result.

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/google/uuid"
	"google.golang.org/genproto/googleapis/bytestream"
	"google.golang.org/grpc/codes"
)

// bndSendWrite streams payload in chunks of the given sizes (in order; a
// final chunk covers any remaining bytes) and returns the final response.
func bndSendWrite(t *testing.T, fx *bndFixture, rn string, payload []byte,
	chunkSizes []int, writeOffset int64) (*bytestream.WriteResponse, error) {
	t.Helper()

	wc, err := fx.bs.Write(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	if len(chunkSizes) == 0 {
		chunkSizes = []int{len(payload)}
	}

	pos := 0
	first := true
	ci := 0
	for {
		n := len(payload) - pos // default: all remaining
		if ci < len(chunkSizes) {
			n = chunkSizes[ci]
			ci++
		}
		if n > len(payload)-pos {
			n = len(payload) - pos
		}

		req := &bytestream.WriteRequest{Data: payload[pos : pos+n]}
		if first {
			req.ResourceName = rn
			req.WriteOffset = writeOffset
			first = false
		}
		pos += n
		if pos == len(payload) {
			req.FinishWrite = true
		}
		if err := wc.Send(req); err != nil && err != io.EOF {
			// The server may fail early; CloseAndRecv surfaces the status.
			break
		}
		if pos == len(payload) {
			break
		}
	}

	return wc.CloseAndRecv()
}

func TestBnd15778_WriteOffsetBoundaries(t *testing.T) {
	fx := bndNewFixture(t, "")
	data, hash := bndRandomData(10)
	fx.put(t, hash, 10, data)

	t.Run("missing-blob-nonzero-offset-rejected", func(t *testing.T) {
		_, missingHash := bndRandomData(10)
		rn := fmt.Sprintf("uploads/%s/blobs/%s/10", uuid.NewString(), missingHash)

		wc, err := fx.bs.Write(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if err := wc.Send(&bytestream.WriteRequest{
			ResourceName: rn,
			WriteOffset:  3,
			Data:         data[:3],
		}); err != nil {
			t.Fatalf("first send: %v", err)
		}
		_, err = wc.CloseAndRecv()
		if bndStatusCode(err) != codes.Unknown {
			t.Fatalf("expected code Unknown for errWriteOffset sentinel, got %s (%v)",
				bndStatusCode(err), err)
		}
		if err == nil || !strings.Contains(err.Error(),
			"bytestream writes from non-zero offsets are unsupported") {
			t.Fatalf("expected errWriteOffset message, got %v", err)
		}
	})

	t.Run("existing-identity-blob-nonzero-offset-returns-size", func(t *testing.T) {
		rn := fmt.Sprintf("uploads/%s/blobs/%s/10", uuid.NewString(), hash)
		resp, err := bndSendWrite(t, fx, rn, data, nil, 7)
		if err != nil {
			t.Fatalf("already-existing blob should succeed, got %v", err)
		}
		if resp.CommittedSize != 10 {
			t.Fatalf("expected CommittedSize 10, got %d", resp.CommittedSize)
		}
	})

	t.Run("existing-compressed-blob-nonzero-offset-returns-minus-one", func(t *testing.T) {
		// The stored blob is the same CAS digest; the compressed resource
		// name only changes the response convention.
		rn := fmt.Sprintf("uploads/%s/compressed-blobs/zstd/%s/10",
			uuid.NewString(), hash)
		compressed := bndZstdEncode(t, data)
		resp, err := bndSendWrite(t, fx, rn, compressed, nil, 7)
		if err != nil {
			t.Fatalf("already-existing compressed blob should succeed, got %v", err)
		}
		if resp.CommittedSize != -1 {
			t.Fatalf("expected CommittedSize -1 for compressed skip, got %d",
				resp.CommittedSize)
		}
	})
}

func TestBnd15778_QueryWriteStatusShape(t *testing.T) {
	fx := bndNewFixture(t, "")
	data, hash := bndRandomData(10)
	rn := fmt.Sprintf("bnd-instance/uploads/%s/blobs/%s/10",
		uuid.NewString(), hash)

	q, err := fx.bs.QueryWriteStatus(context.Background(),
		&bytestream.QueryWriteStatusRequest{ResourceName: rn})
	if err != nil {
		t.Fatal(err)
	}
	if q.Complete || q.CommittedSize != 0 {
		t.Fatalf("missing blob must be {0,false}, got committed=%d complete=%v",
			q.CommittedSize, q.Complete)
	}

	// A QueryWriteStatus on a resource with an invalid name is InvalidArgument.
	_, err = fx.bs.QueryWriteStatus(context.Background(),
		&bytestream.QueryWriteStatusRequest{ResourceName: "not-a-resource"})
	if bndStatusCode(err) != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument for bad name, got %s", bndStatusCode(err))
	}

	// After a successful chunked write, status is {size,true}.
	resp, err := bndSendWrite(t, fx, rn, data, []int{4, 6}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if resp.CommittedSize != 10 {
		t.Fatalf("committed %d", resp.CommittedSize)
	}

	q, err = fx.bs.QueryWriteStatus(context.Background(),
		&bytestream.QueryWriteStatusRequest{ResourceName: rn})
	if err != nil {
		t.Fatal(err)
	}
	if !q.Complete || q.CommittedSize != 10 {
		t.Fatalf("written blob must be {10,true}, got committed=%d complete=%v",
			q.CommittedSize, q.Complete)
	}
}

func TestBnd15778_WriteIdentityOversize(t *testing.T) {
	for _, chunks := range [][]int{{11}, {6, 5}, {1, 1, 1, 1, 1, 1, 5}} {
		fx := bndNewFixture(t, "")
		payload, hash := bndRandomData(11) // declares 10 in the resource name
		rn := bndUploadResource(hash, 10, false)

		_, err := bndSendWrite(t, fx, rn, payload, chunks, 0)
		if bndStatusCode(err) != codes.OutOfRange {
			t.Fatalf("chunks %v: expected OutOfRange, got %s (%v)",
				chunks, bndStatusCode(err), err)
		}
		bndWaitNoHashFiles(t, fx.dir, hash)

		// QueryWriteStatus must not imply a resumable partial upload.
		q, qerr := fx.bs.QueryWriteStatus(context.Background(),
			&bytestream.QueryWriteStatusRequest{ResourceName: rn})
		if qerr != nil {
			t.Fatal(qerr)
		}
		if q.Complete || q.CommittedSize != 0 {
			t.Fatalf("rejected oversize write must report {0,false}, got %+v", q)
		}
	}
}

func TestBnd15778_WriteCompressedSizeMismatch(t *testing.T) {
	cases := []struct {
		name       string
		declared   int64
		actualSize int
	}{
		{"decompressed-longer-than-declared", 10, 25},
		{"decompressed-shorter-than-declared", 25, 10},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fx := bndNewFixture(t, "")
			payload, _ := bndRandomData(int64(tc.actualSize))
			compressed := bndZstdEncode(t, payload)

			// Format-valid hash; size check fires before hash verification.
			_, hash := bndRandomData(32)
			rn := bndUploadResource(hash, tc.declared, true)

			// Send the compressed bytes in odd chunks.
			chunks := []int{3, 7, 1, len(compressed)}
			_, err := bndSendWrite(t, fx, rn, compressed, chunks, 0)

			// Pinned current behavior: disk cache classifies the bad input as
			// an internal cache error, which gRPCErrCode maps to Internal.
			if bndStatusCode(err) != codes.Internal {
				t.Fatalf("expected Internal for compressed size mismatch, got %s (%v)",
					bndStatusCode(err), err)
			}

			// The short case is only noticed once the stream ends / the
			// decoder reaches EOF; either way nothing is committed and no
			// temp file remains.
			bndWaitNoHashFiles(t, fx.dir, hash)

			q, qerr := fx.bs.QueryWriteStatus(context.Background(),
				&bytestream.QueryWriteStatusRequest{ResourceName: rn})
			if qerr != nil {
				t.Fatal(qerr)
			}
			if q.Complete || q.CommittedSize != 0 {
				t.Fatalf("mismatched compressed write must be {0,false}, got %+v", q)
			}
		})
	}
}

// A correctly sized compressed upload succeeds regardless of send chunking
// and reads back identically through both resource name forms.
func TestBnd15778_WriteCompressedHappyPathChunking(t *testing.T) {
	fx := bndNewFixture(t, "")
	data, hash := bndRandomData(4096)
	compressed := bndZstdEncode(t, data)
	rn := bndUploadResource(hash, int64(len(data)), true)

	resp, err := bndSendWrite(t, fx, rn, compressed, []int{100, 1000, 7, 5000}, 0)
	if err != nil {
		t.Fatalf("compressed write: %v", err)
	}
	if resp.CommittedSize != int64(len(compressed)) {
		// CommittedSize counts received (compressed wire) bytes.
		t.Fatalf("expected committed compressed bytes %d, got %d",
			len(compressed), resp.CommittedSize)
	}

	out := bndRead(t, fx.bs, bndBlobResource(hash, int64(len(data))), 0, 0)
	if out.code != codes.OK || string(out.data) != string(data) {
		t.Fatalf("uncompressed readback failed code=%s len=%d", out.code, len(out.data))
	}
	zout := bndRead(t, fx.bs, bndCompressedResource(hash, int64(len(data))), 0, 0)
	if zout.code != codes.OK {
		t.Fatalf("compressed readback code=%s (%s)", zout.code, zout.msg)
	}
	if got := bndZstdDecode(t, zout.data); !bytes.Equal(got, data) {
		t.Fatalf("compressed readback content mismatch")
	}
}
