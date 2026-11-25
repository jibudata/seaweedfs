package filer

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

// Regression for the manifestize fold ordering (lab finding F2, 2026-11-21):
// folding batches chunks in LIST order, which for random-overwrite workloads
// is upload-completion order - far-apart chunks land in one manifest whose
// declared span (min..max offset of the batch) is many times the sub-chunks'
// actual coverage.  Views built from such manifests collapse.  Batches must
// be offset-contiguous after the fix.

func TestF2FoldBatchesAreOffsetContiguous(t *testing.T) {
	M := int64(1024 * 1024)
	// 8 batches worth (ManifestBatch=8 in tests? use doMaybeManifestize with
	// mergeFactor=4 for a small fixture) of chunks at scattered offsets in
	// upload-completion order.
	input := []*filer_pb.FileChunk{}
	// offsets: 6M, 0, 4M, 2M, 7M, 1M, 5M, 3M (each 1M) - deliberately scrambled
	for _, off := range []int64{6 * M, 0, 4 * M, 2 * M, 7 * M, 1 * M, 5 * M, 3 * M} {
		input = append(input, &filer_pb.FileChunk{
			FileId:       "x",
			Offset:       off,
			Size:         uint64(M),
			ModifiedTsNs: 100,
		})
	}

	var savedSpans []spanProbe
	save := func(reader interface{ Read([]byte) (int, error) }, name string, offset int64, tsNs int64, expected uint64) (*filer_pb.FileChunk, error) {
		// not exercised: mergeIntoManifest uses saveDataAsChunk-shaped fn
		return nil, nil
	}
	_ = save

	var blobs [][]*filer_pb.FileChunk
	mergefn := func(saveFunc SaveDataAsChunkFunctionType, dataChunks []*filer_pb.FileChunk) (*filer_pb.FileChunk, error) {
		blobs = append(blobs, append([]*filer_pb.FileChunk{}, dataChunks...))
		minOff, maxOff := int64(-1), int64(-1)
		for _, c := range dataChunks {
			if minOff < 0 || c.Offset < minOff {
				minOff = c.Offset
			}
			if e := c.Offset + int64(c.Size); e > maxOff {
				maxOff = e
			}
		}
		return &filer_pb.FileChunk{
			FileId:          "mf",
			Offset:          minOff,
			Size:            uint64(maxOff - minOff),
			IsChunkManifest: true,
		}, nil
	}
	out, err := doMaybeManifestize(nil, input, 4, mergefn)
	if err != nil {
		t.Fatalf("doMaybeManifestize: %v", err)
	}
	if len(blobs) != 2 {
		t.Fatalf("expected 2 folded batches, got %d", len(blobs))
	}
	for bi, blob := range blobs {
		lo, hi := int64(-1), int64(-1)
		for _, c := range blob {
			if lo < 0 || c.Offset < lo {
				lo = c.Offset
			}
			if e := c.Offset + int64(c.Size); e > hi {
				hi = e
			}
		}
		covered := int64(0)
		for _, c := range blob {
			if c.Offset < lo || c.Offset+int64(c.Size) > hi {
				t.Fatalf("batch %d: chunk [%d,%d) escapes manifest span [%d,%d)", bi, c.Offset, c.Offset+int64(c.Size), lo, hi)
			}
			covered += int64(c.Size)
		}
		span := hi - lo
		// each batch must be contiguous: span == sum of sub-chunk sizes
		if span != covered {
			t.Errorf("batch %d: span %d != covered %d (gaps/overlap in folded batch)", bi, span, covered)
		}
		savedSpans = append(savedSpans, spanProbe{lo, hi})
	}
	// batches must not overlap and must together cover [0, 8M)
	if savedSpans[0].stop > savedSpans[1].start && savedSpans[1].stop > savedSpans[0].start {
		t.Errorf("batches overlap: [%d,%d) x [%d,%d)", savedSpans[0].start, savedSpans[0].stop, savedSpans[1].start, savedSpans[1].stop)
	}
	_ = out
}

type spanProbe struct{ start, stop int64 }
