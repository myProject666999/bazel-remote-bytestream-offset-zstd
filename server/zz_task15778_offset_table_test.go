package server_test

// The seven required offset boundary rows, each as an independent subtest,
// exercised with both resource name forms (blobs and compressed-blobs/zstd)
// and both ordinary storage modes (zstd casblob and uncompressed legacy).

import (
	"bytes"
	"fmt"
	"testing"

	"google.golang.org/grpc/codes"
)

func TestBnd15778_ReadOffsetBoundaryTable(t *testing.T) {
	const size = int64(10)
	data, hash := bndRandomData(size)

	for _, storageMode := range []string{"", "uncompressed"} {
		modeName := storageMode
		if modeName == "" {
			modeName = "zstd-storage"
		}

		fx := bndNewFixture(t, storageMode)
		fx.put(t, hash, size, data)

		// Five rows for the ten-byte blob. wantLen is the expected
		// (uncompressed) returned byte count.
		rows := []struct {
			name     string
			offset   int64
			wantCode codes.Code
			wantLen  int
			wantMsgs int // -1 = don't assert
		}{
			{"offset0-all-ten", 0, codes.OK, 10, -1},
			{"offset9-last-byte", 9, codes.OK, 1, -1},
			{"offset10-equals-size-empty-ok", 10, codes.OK, 0, 0},
			{"offset11-greater-out-of-range", 11, codes.OutOfRange, 0, -1},
			{"offset-1-negative-invalid-argument", -1, codes.InvalidArgument, 0, -1},
		}

		nameForms := []struct {
			formName   string
			rn         string
			compressed bool
		}{
			{"blobs", bndBlobResource(hash, size), false},
			{"compressed-blobs-zstd", bndCompressedResource(hash, size), true},
		}

		for _, row := range rows {
			for _, form := range nameForms {
				t.Run(fmt.Sprintf("%s/%s/%s", modeName, form.formName, row.name),
					func(t *testing.T) {
						out := bndRead(t, fx.bs, form.rn, row.offset, 0)

						if out.code != row.wantCode {
							t.Fatalf("offset %d: expected code %s, got %s (%s)",
								row.offset, row.wantCode, out.code, out.msg)
						}
						if out.code != codes.OK {
							return
						}

						got := out.data
						if form.compressed {
							got = bndZstdDecode(t, out.data)
						}

						if len(got) != row.wantLen {
							t.Fatalf("offset %d: expected %d bytes, got %d (%q)",
								row.offset, row.wantLen, len(got), got)
						}

						want := data[row.offset:]
						if row.wantLen == 0 {
							want = nil
						}
						if !bytes.Equal(got, want) {
							t.Fatalf("offset %d: content mismatch\nwant %v\ngot  %v",
								row.offset, want, got)
						}

						if row.wantMsgs >= 0 && out.msgs != row.wantMsgs {
							t.Fatalf("offset %d: expected %d ReadResponse messages, got %d",
								row.offset, row.wantMsgs, out.msgs)
						}
					})
			}
		}

		// Two rows for the zero-byte blob. The hash must be the canonical
		// empty SHA256.
		zeroRows := []struct {
			name       string
			offset     int64
			rn         string
			compressed bool
			wantCode   codes.Code
		}{
			{"zero-offset0-empty", 0,
				bndBlobResource(bnd15778EmptySHA256, 0), false, codes.OK},
			{"zero-offset1-out-of-range", 1,
				bndBlobResource(bnd15778EmptySHA256, 0), false, codes.OutOfRange},
			{"zero-offset0-empty-compressed-name", 0,
				bndCompressedResource(bnd15778EmptySHA256, 0), true, codes.OK},
			{"zero-offset1-out-of-range-compressed-name", 1,
				bndCompressedResource(bnd15778EmptySHA256, 0), true, codes.OutOfRange},
		}

		for _, row := range zeroRows {
			t.Run(fmt.Sprintf("%s/%s", modeName, row.name), func(t *testing.T) {
				out := bndRead(t, fx.bs, row.rn, row.offset, 0)
				if out.code != row.wantCode {
					t.Fatalf("expected code %s, got %s (%s)",
						row.wantCode, out.code, out.msg)
				}
				if row.wantCode != codes.OK {
					return
				}

				if row.compressed && row.offset == 0 {
					// Preserved special case: one conventional empty zstd frame.
					wantEmptyZstd := []byte{40, 181, 47, 253, 32, 0, 1, 0, 0}
					if !bytes.Equal(out.data, wantEmptyZstd) {
						t.Fatalf("expected empty zstd frame %v, got %v",
							wantEmptyZstd, out.data)
					}
					if got := bndZstdDecode(t, out.data); len(got) != 0 {
						t.Fatalf("expected decoded empty blob, got %d bytes", len(got))
					}
					return
				}

				if len(out.data) != 0 {
					t.Fatalf("expected no bytes for empty blob read, got %d", len(out.data))
				}
			})
		}

		// Consistency extras: a negative offset on the zero blob is also
		// InvalidArgument, under both resource names.
		for _, rn := range []string{
			bndBlobResource(bnd15778EmptySHA256, 0),
			bndCompressedResource(bnd15778EmptySHA256, 0),
		} {
			t.Run(fmt.Sprintf("%s/zero-offset-negative/%s", modeName, rn[:24]),
				func(t *testing.T) {
					out := bndRead(t, fx.bs, rn, -1, 0)
					if out.code != codes.InvalidArgument {
						t.Fatalf("expected InvalidArgument, got %s (%s)",
							out.code, out.msg)
					}
				})
		}
	}
}
