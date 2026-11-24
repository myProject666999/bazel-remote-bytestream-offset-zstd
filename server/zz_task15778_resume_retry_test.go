package server_test

// Explicit Bazel-style resume scenario: download the whole blob, then retry
// with read_offset equal to the number of bytes received (== size). This can
// happen when the stream fails after all bytes arrived but before clean
// completion. The retry must be an immediate empty OK rather than an error.

import (
	"context"
	"io"
	"testing"

	"google.golang.org/genproto/googleapis/bytestream"
	"google.golang.org/grpc/codes"
)

func TestBnd15778_RetryAtSizeAfterFullDownload(t *testing.T) {
	for _, storageMode := range []string{"", "uncompressed"} {
		fx := bndNewFixture(t, storageMode)

		const size = int64(37)
		data, hash := bndRandomData(size)
		fx.put(t, hash, size, data)

		for _, rn := range []string{
			bndBlobResource(hash, size),
			bndCompressedResource(hash, size),
		} {
			// First attempt: full download, accumulating received bytes.
			stream, err := fx.bs.Read(context.Background(), &bytestream.ReadRequest{ResourceName: rn})
			if err != nil {
				t.Fatal(err)
			}
			var received int64
			for {
				resp, err := stream.Recv()
				if err == io.EOF {
					break
				}
				if err != nil {
					t.Fatalf("initial read: %v", err)
				}
				received += int64(len(resp.Data))
			}
			if received != size && rn == bndBlobResource(hash, size) {
				t.Fatalf("expected %d raw bytes, got %d", size, received)
			}

			// Retry with the offset a client would use.
			out := bndRead(t, fx.bs, rn, size, 0)
			if out.code != codes.OK {
				t.Fatalf("storage=%q retry at offset=size: expected OK empty stream, got %s (%s)",
					storageMode, out.code, out.msg)
			}
			if out.msgs != 0 || len(out.data) != 0 {
				t.Fatalf("retry at size must send zero messages, got %d messages %d bytes",
					out.msgs, len(out.data))
			}
		}
	}
}
