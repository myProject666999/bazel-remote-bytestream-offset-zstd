package server_test

// Boundary-pinning tests for the ByteStream Read/Write/QueryWriteStatus
// entry points. This file only contains the shared in-process bufconn
// fixture and helpers; the actual table driven cases live in the
// sibling zz_task15778_*_test.go files.
//
// The unique bnd15778 prefix on every identifier is intentional: these
// tests coexist with any other in-package server test files.

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/genproto/googleapis/bytestream"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/buchgr/bazel-remote/v2/cache"
	"github.com/buchgr/bazel-remote/v2/cache/disk"
	pb "github.com/buchgr/bazel-remote/v2/genproto/build/bazel/remote/execution/v2"
	brserver "github.com/buchgr/bazel-remote/v2/server"
	testutils "github.com/buchgr/bazel-remote/v2/utils"

	"github.com/klauspost/compress/zstd"
)

const bnd15778EmptySHA256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

// Must match cache/disk/casblob.defaultChunkSize (unexported there).
const bnd15778ChunkSize = int64(1024 * 1024)

const bnd15778MaxCasBlobSize = int64(123456789)

type bndFixture struct {
	dir    string
	dc     disk.Cache
	bs     bytestream.ByteStreamClient
	caps   pb.CapabilitiesClient
	lis    *bufconn.Listener
	server *grpc.Server
	conn   *grpc.ClientConn
}

// bndNewFixture starts an in-process gRPC server backed by a temporary
// disk cache. storageMode is "" (default, zstd casblob storage) or
// "uncompressed" (legacy .v1 raw files).
func bndNewFixture(t *testing.T, storageMode string) *bndFixture {
	t.Helper()
	return bndNewFixtureWithLog(t, storageMode, io.Discard)
}

// bndNewFixtureWithLog is bndNewFixture but writes the access logger to w
// (useful for asserting on formatted log lines).
func bndNewFixtureWithLog(t *testing.T, storageMode string, w io.Writer) *bndFixture {
	t.Helper()

	dir := t.TempDir()

	opts := []disk.Option{disk.WithAccessLogger(testutils.NewSilentLogger())}
	if storageMode != "" {
		opts = append(opts, disk.WithStorageMode(storageMode))
	}

	dc, err := disk.New(dir, 1<<30, opts...)
	if err != nil {
		t.Fatalf("disk.New failed: %v", err)
	}

	return bndServeCacheWithLog(t, dir, dc, w)
}

// bndServeCache wraps an existing disk cache with a bufconn gRPC server.
func bndServeCache(t *testing.T, dir string, dc disk.Cache) *bndFixture {
	t.Helper()
	return bndServeCacheWithLog(t, dir, dc, io.Discard)
}

func bndServeCacheWithLog(t *testing.T, dir string, dc disk.Cache,
	logWriter io.Writer) *bndFixture {
	t.Helper()

	accessLogger := testutils.NewSilentLogger()
	if logWriter != io.Discard {
		accessLogger = log.New(logWriter, "", 0)
	}

	lis := bufconn.Listen(4 * 1024 * 1024)
	gs := grpc.NewServer()

	go func() {
		_ = brserver.ServeGRPC(lis, gs, true, false, true,
			bnd15778MaxCasBlobSize, dc,
			accessLogger, testutils.NewSilentLogger())
	}()

	conn, err := grpc.NewClient("passthrough://bufnet",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lis.Dial()
		}))
	if err != nil {
		t.Fatalf("grpc.NewClient failed: %v", err)
	}

	t.Cleanup(func() {
		_ = conn.Close()
		gs.Stop()
	})

	return &bndFixture{
		dir:    dir,
		dc:     dc,
		bs:     bytestream.NewByteStreamClient(conn),
		caps:   pb.NewCapabilitiesClient(conn),
		lis:    lis,
		server: gs,
		conn:   conn,
	}
}

func bndHash(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

var bndRng = rand.New(rand.NewSource(15778))

// bndRandomData returns n bytes of deterministic random data and its hash.
func bndRandomData(n int64) ([]byte, string) {
	b := make([]byte, n)
	_, _ = bndRng.Read(b)
	return b, bndHash(b)
}

func (f *bndFixture) put(t *testing.T, hash string, size int64, data []byte) {
	t.Helper()
	if err := f.dc.Put(context.Background(), cache.CAS, hash, size, bytes.NewReader(data)); err != nil {
		t.Fatalf("cache.Put(%s, %d) failed: %v", hash, size, err)
	}
}

func bndBlobResource(hash string, size int64) string {
	return fmt.Sprintf("bnd-instance/blobs/%s/%d", hash, size)
}

func bndCompressedResource(hash string, size int64) string {
	return fmt.Sprintf("bnd-instance/compressed-blobs/zstd/%s/%d", hash, size)
}

func bndUploadResource(hash string, size int64, compressed bool, extra ...string) string {
	kind := "blobs"
	comp := ""
	if compressed {
		kind = "compressed-blobs"
		comp = "/zstd"
	}
	rn := fmt.Sprintf("bnd-instance/uploads/bnd-uuid-15778/%s%s/%s/%d",
		kind, comp, hash, size)
	for _, seg := range extra {
		rn += "/" + seg
	}
	return rn
}

type bndReadOut struct {
	data []byte
	code codes.Code
	msg  string
	msgs int
}

func bndStatusCode(err error) codes.Code {
	if err == nil {
		return codes.OK
	}
	return status.Code(err)
}

// bndRead performs a ByteStream Read and collects the raw payload and the
// terminal gRPC status. io.EOF is translated to codes.OK.
func bndRead(t *testing.T, c bytestream.ByteStreamClient, resource string,
	offset, limit int64) bndReadOut {
	t.Helper()

	stream, err := c.Read(context.Background(), &bytestream.ReadRequest{
		ResourceName: resource,
		ReadOffset:   offset,
		ReadLimit:    limit,
	})
	if err != nil {
		st, _ := status.FromError(err)
		return bndReadOut{code: st.Code(), msg: st.Message()}
	}

	var out bndReadOut
	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			if out.code == 0 {
				out.code = codes.OK
			}
			return out
		}
		if err != nil {
			st, _ := status.FromError(err)
			out.code = st.Code()
			out.msg = st.Message()
			return out
		}
		if resp == nil {
			continue
		}
		out.msgs++
		out.data = append(out.data, resp.Data...)
	}
}

// bndZstdDecode decodes a full (possibly concatenated/re-encoded) zstd
// stream. An empty input decodes to an empty byte slice.
func bndZstdDecode(t *testing.T, raw []byte) []byte {
	t.Helper()
	if len(raw) == 0 {
		return nil
	}
	dec, err := zstd.NewReader(bytes.NewReader(raw), zstd.WithDecoderConcurrency(1))
	if err != nil {
		t.Fatalf("zstd.NewReader: %v", err)
	}
	out, err := io.ReadAll(dec)
	if err != nil {
		t.Fatalf("zstd decode: %v", err)
	}
	dec.Close()
	return out
}

// bndCountHashFiles lists files anywhere under dir whose path contains hash.
func bndCountHashFiles(t *testing.T, dir, hash string) []string {
	t.Helper()
	var hits []string
	err := filepath.Walk(dir, func(p string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return nil
		}
		if bytes.Contains([]byte(p), []byte(hash)) {
			hits = append(hits, p)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk %s: %v", dir, err)
	}
	return hits
}

// bndWaitNoHashFiles polls until no file path under dir contains hash,
// because failed writes clean their temp file up asynchronously relative
// to the client receiving the final status.
func bndWaitNoHashFiles(t *testing.T, dir, hash string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		hits := bndCountHashFiles(t, dir, hash)
		if len(hits) == 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	hits := bndCountHashFiles(t, dir, hash)
	t.Fatalf("expected no files for hash %s, still found %v", hash, hits)
}

func bndZstdEncode(t *testing.T, data []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	enc, err := zstd.NewWriter(&buf, zstd.WithEncoderConcurrency(1))
	if err != nil {
		t.Fatalf("zstd.NewWriter: %v", err)
	}
	if _, err := enc.Write(data); err != nil {
		t.Fatalf("zstd write: %v", err)
	}
	if err := enc.Close(); err != nil {
		t.Fatalf("zstd close: %v", err)
	}
	return buf.Bytes()
}
