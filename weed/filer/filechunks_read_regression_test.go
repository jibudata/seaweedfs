package filer

import (
	"fmt"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

// Regression tests for the readResolvedChunks view builder (lab finding F2,
// 2026-11-20): every chunk in the input must remain visible (fully, or by
// newer overlays) in the output view.  A chunk whose START point hits the
// queue-insert else branch (ts equal to a queued ts, or ts lower than every
// queued ts) was silently dropped from the queue, its coverage vanished from
// the view, and the mount sync then deleted the chunk from the filer entry
// as "garbage" - silently losing acked writes.

func f2mkChunk(offset, size, ts int64, tag string) *filer_pb.FileChunk {
	return &filer_pb.FileChunk{
		FileId:         tag,
		Offset:         offset,
		Size:           uint64(size),
		ModifiedTsNs:   ts,
		IsChunkManifest: false,
	}
}

func f2coverage(t *testing.T, name string, chunks []*filer_pb.FileChunk, fileSize int64) (map[string]int64, int64, bool) {
	t.Helper()
	visibles := readResolvedChunks(chunks, 0, fileSize)
	byId := map[string]int64{}
	var covered int64
	var cursor int64
	hole := int64(-1)
	for x := visibles.Front(); x != nil; x = x.Next {
		v := x.Value
		byId[v.fileId] += v.stop - v.start
		if v.start > cursor {
			hole = cursor
		}
		if v.stop > cursor {
			cursor = v.stop
		}
	}
	_ = covered
	return byId, hole, haveHoleFlag(haveHoleBool(hole >= 0))
}

func haveHoleBool(b bool) bool { return b }

func haveHoleFlag(b bool) bool { return b }

func f2expectVisible(t *testing.T, byId map[string]int64, tag string) {
	t.Helper()
	if byId[tag] == 0 {
		t.Errorf("chunk %s vanished from the view (byId=%v)", tag, byId)
	}
}

// Random-overwrite shape: an early base chunk, then later overlay chunks at
// scattered offsets with HIGHER ts (normal out-of-order upload completion).
func TestF2ReadResolvedChunks_OutOfOrderOverlaysStayVisible(t *testing.T) {
	M := int64(1024 * 1024)
	chunks := []*filer_pb.FileChunk{
		f2mkChunk(0, 8*M, 100, "base"),
		f2mkChunk(1*M, 256*1024, 200, "ov1"),
		f2mkChunk(3*M, 256*1024, 300, "ov2"),
		f2mkChunk(5*M, 256*1024, 400, "ov3"),
		f2mkChunk(7*M, 256*1024, 500, "ov4"),
	}
	visibles := readResolvedChunks(chunks, 0, 8*M)
	byId := map[string]int64{}
	for x := visibles.Front(); x != nil; x = x.Next {
		v := x.Value
		byId[v.fileId] += v.stop - v.start
	}
	for _, tag := range []string{"base", "ov1", "ov2", "ov3", "ov4"} {
		f2expectVisible(t, byId, tag)
	}
}

// THE F2 KILLER: equal-timestamp overlapping chunks. Two writes flushed at
// the same ModifiedTsNs must both remain in the view - the newer-position
// chunk wins per sub-range, but nothing may be dropped.
func TestF2ReadResolvedChunks_EqualTsOverlapStaysVisible(t *testing.T) {
	M := int64(1024 * 1024)
	chunks := []*filer_pb.FileChunk{
		f2mkChunk(0, 8*M, 500, "base"),
		f2mkChunk(1*M, 1*M, 500, "ovA"),
		f2mkChunk(2*M, 1*M, 500, "ovB"),
	}
	visibles := readResolvedChunks(chunks, 0, 8*M)
	byId := map[string]int64{}
	for x := visibles.Front(); x != nil; x = x.Next {
		v := x.Value
		byId[v.fileId] += v.stop - v.start
	}
	for _, tag := range []string{"base", "ovA", "ovB"} {
		f2expectVisible(t, byId, tag)
	}
}

// Out-of-order arrival: a chunk whose ts is LOWER than an already-queued
// chunk (upload completion order != write order). Its (non-overlapping)
// coverage must still appear.
func TestF2ReadResolvedChunks_LateTsChunkStaysVisible(t *testing.T) {
	M := int64(1024 * 1024)
	chunks := []*filer_pb.FileChunk{
		f2mkChunk(5*M, 1*M, 600, "earlyQueued"),
		f2mkChunk(4*M, 1*M, 300, "lateArrival"),
	}
	visibles := readResolvedChunks(chunks, 0, 8*M)
	byId := map[string]int64{}
	for x := visibles.Front(); x != nil; x = x.Next {
		v := x.Value
		byId[v.fileId] += v.stop - v.start
	}
	f2expectVisible(t, byId, "earlyQueued")
	f2expectVisible(t, byId, "lateArrival")
}

// Debug helper: print the view for eyeballing.
func TestF2PrintView(t *testing.T) {
	if testing.Short() {
		t.Skip("manual eyeball only")
	}
	M := int64(1024 * 1024)
	chunks := []*filer_pb.FileChunk{
		f2mkChunk(0, 8*M, 100, "base"),
		f2mkChunk(1*M, 256*1024, 200, "ov1"),
	}
	visibles := readResolvedChunks(chunks, 0, 8*M)
	for x := visibles.Front(); x != nil; x = x.Next {
		v := x.Value
		fmt.Printf("visible [%d,%d) fileId=%s ts=%d\n", v.start, v.stop, v.fileId, v.modifiedTsNs)
	}
}
