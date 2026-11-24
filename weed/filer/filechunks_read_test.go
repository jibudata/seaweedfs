package filer

import (
	"fmt"
	"math"
	"math/rand"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

func TestReadResolvedChunks(t *testing.T) {

	chunks := []*filer_pb.FileChunk{
		{
			FileId:       "a",
			Offset:       0,
			Size:         100,
			ModifiedTsNs: 1,
		},
		{
			FileId:       "b",
			Offset:       50,
			Size:         100,
			ModifiedTsNs: 2,
		},
		{
			FileId:       "c",
			Offset:       200,
			Size:         50,
			ModifiedTsNs: 3,
		},
		{
			FileId:       "d",
			Offset:       250,
			Size:         50,
			ModifiedTsNs: 4,
		},
		{
			FileId:       "e",
			Offset:       175,
			Size:         100,
			ModifiedTsNs: 5,
		},
	}

	visibles := readResolvedChunks(chunks, 0, math.MaxInt64)

	fmt.Printf("resolved to %d visible intervales\n", visibles.Len())
	for x := visibles.Front(); x != nil; x = x.Next {
		visible := x.Value
		fmt.Printf("[%d,%d) %s %d\n", visible.start, visible.stop, visible.fileId, visible.modifiedTsNs)
	}

}

func TestReadResolvedChunks2(t *testing.T) {

	chunks := []*filer_pb.FileChunk{
		{
			FileId:       "c",
			Offset:       200,
			Size:         50,
			ModifiedTsNs: 3,
		},
		{
			FileId:       "e",
			Offset:       200,
			Size:         25,
			ModifiedTsNs: 5,
		},
	}

	visibles := readResolvedChunks(chunks, 0, math.MaxInt64)

	fmt.Printf("resolved to %d visible intervales\n", visibles.Len())
	for x := visibles.Front(); x != nil; x = x.Next {
		visible := x.Value
		fmt.Printf("[%d,%d) %s %d\n", visible.start, visible.stop, visible.fileId, visible.modifiedTsNs)
	}

}

func TestRandomizedReadResolvedChunks(t *testing.T) {

	var limit int64 = 1024 * 1024
	array := make([]int64, limit)
	var chunks []*filer_pb.FileChunk
	for ts := int64(0); ts < 1024; ts++ {
		x := rand.Int63n(limit)
		y := rand.Int63n(limit)
		size := x - y
		if size < 0 {
			size = -size
		}
		if size > 1024 {
			size = 1024
		}
		start := x
		if start > y {
			start = y
		}
		chunks = append(chunks, randomWrite(array, start, size, ts))
	}

	visibles := readResolvedChunks(chunks, 0, math.MaxInt64)

	for x := visibles.Front(); x != nil; x = x.Next {
		visible := x.Value
		for i := visible.start; i < visible.stop; i++ {
			if array[i] != visible.modifiedTsNs {
				t.Errorf("position %d expected ts %d actual ts %d", i, array[i], visible.modifiedTsNs)
			}
		}
	}

	// fmt.Printf("visibles %d", len(visibles))

}

func randomWrite(array []int64, start int64, size int64, ts int64) *filer_pb.FileChunk {
	for i := start; i < start+size; i++ {
		array[i] = ts
	}
	// fmt.Printf("write [%d,%d) %d\n", start, start+size, ts)
	return &filer_pb.FileChunk{
		FileId:       "",
		Offset:       start,
		Size:         uint64(size),
		ModifiedTsNs: ts,
	}
}

func TestSequentialReadResolvedChunks(t *testing.T) {

	var chunkSize int64 = 1024 * 1024 * 2
	var chunks []*filer_pb.FileChunk
	for ts := int64(0); ts < 13; ts++ {
		chunks = append(chunks, &filer_pb.FileChunk{
			FileId:       "",
			Offset:       chunkSize * ts,
			Size:         uint64(chunkSize),
			ModifiedTsNs: 1,
		})
	}

	visibles := readResolvedChunks(chunks, 0, math.MaxInt64)

	fmt.Printf("visibles %d", visibles.Len())

}

func TestActualReadResolvedChunks(t *testing.T) {

	chunks := []*filer_pb.FileChunk{
		{
			FileId:       "5,e7b96fef48",
			Offset:       0,
			Size:         2097152,
			ModifiedTsNs: 1634447487595823000,
		},
		{
			FileId:       "5,e5562640b9",
			Offset:       2097152,
			Size:         2097152,
			ModifiedTsNs: 1634447487595826000,
		},
		{
			FileId:       "5,df033e0fe4",
			Offset:       4194304,
			Size:         2097152,
			ModifiedTsNs: 1634447487595827000,
		},
		{
			FileId:       "7,eb08148a9b",
			Offset:       6291456,
			Size:         2097152,
			ModifiedTsNs: 1634447487595827000,
		},
		{
			FileId:       "7,e0f92d1604",
			Offset:       8388608,
			Size:         2097152,
			ModifiedTsNs: 1634447487595828000,
		},
		{
			FileId:       "7,e33cb63262",
			Offset:       10485760,
			Size:         2097152,
			ModifiedTsNs: 1634447487595828000,
		},
		{
			FileId:       "5,ea98e40e93",
			Offset:       12582912,
			Size:         2097152,
			ModifiedTsNs: 1634447487595829000,
		},
		{
			FileId:       "5,e165661172",
			Offset:       14680064,
			Size:         2097152,
			ModifiedTsNs: 1634447487595829000,
		},
		{
			FileId:       "3,e692097486",
			Offset:       16777216,
			Size:         2097152,
			ModifiedTsNs: 1634447487595830000,
		},
		{
			FileId:       "3,e28e2e3cbd",
			Offset:       18874368,
			Size:         2097152,
			ModifiedTsNs: 1634447487595830000,
		},
		{
			FileId:       "3,e443974d4e",
			Offset:       20971520,
			Size:         2097152,
			ModifiedTsNs: 1634447487595830000,
		},
		{
			FileId:       "2,e815bed597",
			Offset:       23068672,
			Size:         2097152,
			ModifiedTsNs: 1634447487595831000,
		},
		{
			FileId:       "5,e94715199e",
			Offset:       25165824,
			Size:         1974736,
			ModifiedTsNs: 1634447487595832000,
		},
	}

	visibles := readResolvedChunks(chunks, 0, math.MaxInt64)

	for x := visibles.Front(); x != nil; x = x.Next {
		visible := x.Value
		fmt.Printf("[%d,%d) %s %d\n", visible.start, visible.stop, visible.fileId, visible.modifiedTsNs)
	}

}

func TestActualReadResolvedChunks2(t *testing.T) {

	chunks := []*filer_pb.FileChunk{
		{
			FileId:       "1,e7b96fef48",
			Offset:       0,
			Size:         184320,
			ModifiedTsNs: 1,
		},
		{
			FileId:       "2,22562640b9",
			Offset:       184320,
			Size:         4096,
			ModifiedTsNs: 2,
		},
		{
			FileId:       "2,33562640b9",
			Offset:       184320,
			Size:         4096,
			ModifiedTsNs: 4,
		},
		{
			FileId:       "4,df033e0fe4",
			Offset:       188416,
			Size:         2097152,
			ModifiedTsNs: 3,
		},
	}

	visibles := readResolvedChunks(chunks, 0, math.MaxInt64)

	for x := visibles.Front(); x != nil; x = x.Next {
		visible := x.Value
		fmt.Printf("[%d,%d) %s %d\n", visible.start, visible.stop, visible.fileId, visible.modifiedTsNs)
	}

}

// ---------------------------------------------------------------------------
// Coverage regression tests.
//
// readResolvedChunks must satisfy two invariants:
//
//  1. Coverage completeness: the union of the emitted intervals equals the
//     union of the chunk spans clipped to the window. A dropped range reads
//     back as zeros, and SeparateGarbageChunks classifies any chunk whose
//     coverage was never emitted as garbage — the flush path then deletes it
//     from the filer entry, silently discarding acked writes.
//  2. Newest write wins: each range is attributed to the chunk with the
//     highest ModifiedTsNs spanning it; ties are broken by list order
//     (later appends shadow earlier same-ts chunks, matching the append
//     order in which entry.Chunks accumulates concurrent uploads).
//
// The previous queue-based sweep broke invariant 1 whenever timestamps
// repeated (same-millisecond writes; manifest folds at ModifiedTsNs=0):
// a START point whose ts tied the visible maximum was never inserted into
// the queue, and an END point removed the first queued entry with a
// matching ts, so ended chunks stayed queued as ghosts while their own
// coverage was never emitted.
// ---------------------------------------------------------------------------

// expectedWinners simulates the visible winner per byte position: highest
// ModifiedTsNs wins, later list entries win ties.
func expectedWinners(chunks []*filer_pb.FileChunk) map[int64]*filer_pb.FileChunk {
	winners := make(map[int64]*filer_pb.FileChunk)
	for _, c := range chunks {
		for i := c.Offset; i < c.Offset+int64(c.Size); i++ {
			if prev, ok := winners[i]; !ok || c.ModifiedTsNs >= prev.ModifiedTsNs {
				winners[i] = c
			}
		}
	}
	return winners
}

// assertCoveragePreserved verifies both invariants over the emitted view.
func assertCoveragePreserved(t *testing.T, chunks []*filer_pb.FileChunk, visibles *IntervalList[*VisibleInterval]) {
	t.Helper()

	expected := expectedWinners(chunks)
	seen := make(map[int64]*VisibleInterval)
	var prevStop int64 = math.MinInt64
	for x := visibles.Front(); x != nil; x = x.Next {
		v := x.Value
		if v.start < prevStop {
			t.Fatalf("emitted intervals overlap or are not ascending: [%d,%d) follows [%d,...)", v.start, v.stop, prevStop)
		}
		if v.stop <= v.start {
			t.Fatalf("emitted empty interval [%d,%d)", v.start, v.stop)
		}
		prevStop = v.stop
		for i := v.start; i < v.stop; i++ {
			if _, dup := seen[i]; dup {
				t.Fatalf("position %d covered more than once", i)
			}
			seen[i] = v
			exp, covered := expected[i]
			if !covered {
				t.Fatalf("position %d emitted as %s but no chunk covers it (hole leaked into the view)", i, v.fileId)
			}
			if v.modifiedTsNs != exp.ModifiedTsNs || v.fileId != exp.GetFileIdString() {
				t.Fatalf("position %d: expected winner %s(ts=%d), got %s(ts=%d)",
					i, exp.GetFileIdString(), exp.ModifiedTsNs, v.fileId, v.modifiedTsNs)
			}
		}
	}
	for pos, c := range expected {
		if _, ok := seen[pos]; !ok {
			t.Fatalf("position %d is covered by chunk %s(ts=%d) but missing from the emitted view: coverage loss",
				pos, c.GetFileIdString(), c.ModifiedTsNs)
		}
	}
}

func TestReadResolvedChunksTiedTimestampsCoverage(t *testing.T) {
	// Two overlapping writes carrying the same ModifiedTsNs (the queue sweep
	// dropped the second START on the tie and emitted only chunk a's range).
	chunks := []*filer_pb.FileChunk{
		{FileId: "a", Offset: 0, Size: 100, ModifiedTsNs: 7},
		{FileId: "b", Offset: 50, Size: 100, ModifiedTsNs: 7},
	}
	visibles := readResolvedChunks(chunks, 0, math.MaxInt64)
	assertCoveragePreserved(t, chunks, visibles)
}

func TestReadResolvedChunksZeroTimestampsCoverage(t *testing.T) {
	// Manifest folds write ModifiedTsNs=0; interleaved zero-ts chunks must
	// not lose coverage.
	chunks := []*filer_pb.FileChunk{
		{FileId: "a", Offset: 0, Size: 64, ModifiedTsNs: 0},
		{FileId: "b", Offset: 32, Size: 64, ModifiedTsNs: 0},
		{FileId: "c", Offset: 80, Size: 48, ModifiedTsNs: 0},
	}
	visibles := readResolvedChunks(chunks, 0, math.MaxInt64)
	assertCoveragePreserved(t, chunks, visibles)
}

func TestReadResolvedChunksGarbageOnlyFullyShadowed(t *testing.T) {
	// The data-loss chain: coverage loss makes SeparateGarbageChunks delete
	// chunks that still carry unshadowed (acked) data.
	chunks := []*filer_pb.FileChunk{
		{FileId: "a", Offset: 0, Size: 100, ModifiedTsNs: 7},
		{FileId: "b", Offset: 50, Size: 100, ModifiedTsNs: 7}, // unshadowed [100,150)
	}
	visibles := readResolvedChunks(chunks, 0, math.MaxInt64)
	_, garbage := SeparateGarbageChunks(visibles, chunks)
	if len(garbage) != 0 {
		t.Fatalf("chunk b still carries unshadowed data [100,150) but was classified as garbage (%d chunks)", len(garbage))
	}

	// Fully shadowed variant: a later same-ts full overwrite must shadow the
	// earlier chunk exactly, so the earlier blob is the only garbage.
	chunks2 := []*filer_pb.FileChunk{
		{FileId: "a", Offset: 0, Size: 100, ModifiedTsNs: 7},
		{FileId: "b", Offset: 0, Size: 100, ModifiedTsNs: 7},
	}
	visibles2 := readResolvedChunks(chunks2, 0, math.MaxInt64)
	_, garbage2 := SeparateGarbageChunks(visibles2, chunks2)
	if len(garbage2) != 1 || garbage2[0].GetFileIdString() != "a" {
		t.Fatalf("expected only the fully-shadowed earlier chunk a as garbage, got %d chunks", len(garbage2))
	}
}

func TestReadResolvedChunksCoveragePropertyWithTies(t *testing.T) {
	// Property test over random overlapping writes drawn from a tiny ts
	// domain, so timestamp ties are frequent. Verifies coverage equality,
	// winner correctness, interval disjointness, and that true holes stay
	// out of the view.
	r := rand.New(rand.NewSource(20261123))
	var limit int64 = 4096
	var chunks []*filer_pb.FileChunk
	for i := 0; i < 300; i++ {
		x, y := r.Int63n(limit), r.Int63n(limit)
		start, end := min(x, y), max(x, y)
		if start == end {
			continue
		}
		chunks = append(chunks, &filer_pb.FileChunk{
			FileId:       fmt.Sprintf("c%d", i),
			Offset:       start,
			Size:         uint64(end - start),
			ModifiedTsNs: r.Int63n(6),
		})
	}
	visibles := readResolvedChunks(chunks, 0, math.MaxInt64)
	assertCoveragePreserved(t, chunks, visibles)
}
