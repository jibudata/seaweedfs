package filer

import (
	"slices"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

// readResolvedChunks computes the visible (non-shadowed) coverage of the
// given chunks within [startOffset, stopOffset).
//
// The output must satisfy two invariants:
//
//  1. Coverage completeness: the union of the emitted intervals equals the
//     union of the chunk spans clipped to the window. A range covered by any
//     chunk must appear in the output — a dropped range reads back as zeros,
//     and SeparateGarbageChunks classifies any chunk whose coverage was
//     never emitted as garbage, which the flush path then deletes from the
//     filer entry: acked writes would vanish from the namespace while their
//     blobs remain on the volume servers.
//
//  2. Newest write wins: each emitted interval is attributed to the chunk
//     with the highest ModifiedTsNs spanning it; ties are resolved by list
//     order (later entries shadow earlier same-ts chunks, matching the
//     append order in which entry.Chunks accumulates concurrent uploads).
//
// The implementation walks the elementary segments induced by all chunk
// boundaries in ascending order and emits, per segment, the winning chunk;
// adjacent segments won by the same chunk are merged into one interval.
// True holes (ranges no chunk covers) emit nothing, as before.
//
// The previous queue-based sweep violated invariant 1 whenever timestamps
// repeated — same-millisecond writes and manifest folds (ModifiedTsNs=0)
// make ties routine in random-overwrite workloads: a START point whose ts
// equaled the currently visible maximum was silently never inserted, and an
// END point removed the first queued entry with a matching ts, so the wrong
// entry was removed and ended chunks stayed queued as ghosts while their
// coverage was never emitted.
func readResolvedChunks(chunks []*filer_pb.FileChunk, startOffset int64, stopOffset int64) (visibles *IntervalList[*VisibleInterval]) {

	type bspan struct {
		start, stop int64
		c           *filer_pb.FileChunk
	}
	var spans []bspan
	var bounds []int64
	for _, chunk := range chunks {
		if chunk.IsChunkManifest {
			println("This should not happen! A manifest chunk found:", chunk.GetFileIdString())
		}
		start, stop := max(chunk.Offset, startOffset), min(chunk.Offset+int64(chunk.Size), stopOffset)
		if start >= stop {
			continue
		}
		spans = append(spans, bspan{start: start, stop: stop, c: chunk})
		bounds = append(bounds, start, stop)
	}
	visibles = NewIntervalList[*VisibleInterval]()
	if len(spans) == 0 {
		return visibles
	}
	slices.Sort(bounds)
	bounds = slices.Compact(bounds)

	// Emit per-segment winners, merging adjacent segments won by the same
	// chunk into one interval (readers expect one interval per contiguous
	// same-chunk run).
	var pendChunk *filer_pb.FileChunk
	var pendStart, pendStop int64
	flushPending := func() {
		if pendChunk != nil {
			visibles.AppendInterval(&Interval[*VisibleInterval]{
				StartOffset: pendStart,
				StopOffset:  pendStop,
				TsNs:        pendChunk.ModifiedTsNs,
				Value: &VisibleInterval{
					start:         pendStart,
					stop:          pendStop,
					fileId:        pendChunk.GetFileIdString(),
					modifiedTsNs:  pendChunk.ModifiedTsNs,
					offsetInChunk: pendStart - pendChunk.Offset,
					chunkSize:     pendChunk.Size,
					cipherKey:     pendChunk.CipherKey,
					isGzipped:     pendChunk.IsCompressed,
				},
			})
			pendChunk = nil
		}
	}
	for i := 0; i+1 < len(bounds); i++ {
		lo, hi := bounds[i], bounds[i+1]
		var best *filer_pb.FileChunk
		for si := range spans {
			sp := &spans[si]
			if sp.start <= lo && hi <= sp.stop {
				// newest write wins; ties resolved by list order (later
				// entries are later appends, so they shadow earlier
				// same-ts chunks)
				if best == nil || sp.c.ModifiedTsNs >= best.ModifiedTsNs {
					best = sp.c
				}
			}
		}
		if best != pendChunk {
			flushPending()
			pendChunk = best
			pendStart, pendStop = lo, hi
		} else {
			pendStop = hi
		}
	}
	flushPending()

	return visibles
}
