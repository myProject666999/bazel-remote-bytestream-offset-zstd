package server_test

// The negative-offset / negative-limit log lines used to have two format
// verbs but a single argument, producing "%!s(MISSING)" / "%!d(MISSING)"
// markers. Pin the formatted output and the unchanged log prefixes.

import (
	"bytes"
	"strings"
	"sync"
	"testing"

	"google.golang.org/grpc/codes"
)

type bndSyncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *bndSyncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *bndSyncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func TestBnd15778_ValidationLogFormatting(t *testing.T) {
	var logs bndSyncBuffer
	fx := bndNewFixtureWithLog(t, "", &logs)

	const size = int64(10)
	_, hash := bndRandomData(size)
	rn := bndBlobResource(hash, size)
	zrn := bndCompressedResource(hash, size)

	cases := []struct {
		name       string
		rn         string
		offset     int64
		limit      int64
		wantCode   codes.Code
		wantPrefix string
	}{
		{
			"negative-offset", rn, -1, 0, codes.InvalidArgument,
			"GRPC BYTESTREAM READ OFFSET INVALID:",
		},
		{
			"compressed-nonzero-limit", zrn, 0, 7, codes.InvalidArgument,
			"GRPC BYTESTREAM READ LIMIT INVALID:",
		},
		{
			"negative-limit", rn, 0, -9, codes.OutOfRange,
			"GRPC BYTESTREAM READ LIMIT OUT OF RANGE:",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			out := bndRead(t, fx.bs, tc.rn, tc.offset, tc.limit)
			if out.code != tc.wantCode {
				t.Fatalf("expected %s, got %s (%s)", tc.wantCode, out.code, out.msg)
			}
		})
	}

	joined := logs.String()
	if strings.Contains(joined, "%!") || strings.Contains(joined, "MISSING") {
		t.Fatalf("log output contains format placeholder errors:\n%s", joined)
	}

	for _, prefix := range []string{
		"GRPC BYTESTREAM READ OFFSET INVALID: " + rn + " -1",
		"GRPC BYTESTREAM READ LIMIT INVALID: " + zrn + " 7",
		"GRPC BYTESTREAM READ LIMIT OUT OF RANGE: " + rn + " -9",
	} {
		if !strings.Contains(joined, prefix) {
			t.Fatalf("expected log line containing %q, got:\n%s", prefix, joined)
		}
	}
}
