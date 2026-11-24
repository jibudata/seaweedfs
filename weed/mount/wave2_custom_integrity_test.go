//go:build !freebsd

package mount

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"google.golang.org/protobuf/proto"
)

func TestChunksDiffer(t *testing.T) {
	c1 := &filer_pb.FileChunk{Fid: &filer_pb.FileId{VolumeId: 1, FileKey: 1, Cookie: 1}, Offset: 0, Size: 8}
	c2 := &filer_pb.FileChunk{Fid: &filer_pb.FileId{VolumeId: 1, FileKey: 2, Cookie: 1}, Offset: 8, Size: 8}
	c3 := &filer_pb.FileChunk{Fid: &filer_pb.FileId{VolumeId: 1, FileKey: 3, Cookie: 1}, Offset: 0, Size: 16}

	if chunksDiffer(nil, nil) {
		t.Fatal("empty slices should not differ")
	}
	if chunksDiffer([]*filer_pb.FileChunk{c1, c2}, []*filer_pb.FileChunk{c1, c2}) {
		t.Fatal("identical slices should not differ")
	}
	if chunksDiffer([]*filer_pb.FileChunk{c1, c2}, []*filer_pb.FileChunk{c2, c1}) {
		t.Fatal("reordered slices with same file ids should not differ")
	}
	if !chunksDiffer([]*filer_pb.FileChunk{c1, c2}, []*filer_pb.FileChunk{c2}) {
		t.Fatal("compacted slice dropping a chunk must differ")
	}
	if !chunksDiffer([]*filer_pb.FileChunk{c1, c2}, []*filer_pb.FileChunk{c1, c3}) {
		t.Fatal("slice replacing a chunk must differ")
	}
}

// When CompactFileChunks drops shadowed chunks during flushMetadataToFiler
// without triggering maybeMergeChunks (rewrote == false), fh.entryChunkGroup
// must still be synchronized with the compacted chunk list so open readers
// cannot reference chunks the filer is about to delete/vacuum.
func TestFlushSyncsChunkGroupAfterCompaction(t *testing.T) {
	oldChunk := &filer_pb.FileChunk{
		Fid:          &filer_pb.FileId{VolumeId: 1, FileKey: 10, Cookie: 1},
		Offset:       0,
		Size:         16,
		ModifiedTsNs: 100,
	}
	newChunk := &filer_pb.FileChunk{
		Fid:          &filer_pb.FileId{VolumeId: 1, FileKey: 11, Cookie: 1},
		Offset:       0,
		Size:         16,
		ModifiedTsNs: 200,
	}
	// Make sure shouldMergeChunks is false so flush takes the non-merge path.
	if _, _, bloated := shouldMergeChunks([]*filer_pb.FileChunk{newChunk}, nil); bloated {
		t.Fatal("unexpected bloated result for single compacted chunk")
	}

	base := &filer_pb.Entry{
		Name: "file",
		Attributes: &filer_pb.FuseAttributes{
			Inode:    42,
			FileMode: 0644,
			FileSize: 16,
			Mtime:    time.Now().Unix(),
		},
		Chunks:     []*filer_pb.FileChunk{oldChunk, newChunk},
		CommitTsNs: 100,
	}
	peer := &g6ConditionalPeer{entry: proto.Clone(base).(*filer_pb.Entry)}
	w, handle := g6AppendHandle(t, peer, base, 1000)
	fh := w.GetHandle(FileHandleId(handle))

	// Put ONLY newChunk in the chunk cache; simulate a stale chunkGroup that
	// still points only at oldChunk (which is vacuumed / missing).
	cache := newFakeChunkCache()
	wantPayload := []byte("0123456789abcdef")
	cache.Put(newChunk.GetFileIdString(), wantPayload)
	group, err := filer.NewChunkGroup(func(ctx context.Context, fileId string) ([]string, error) {
		return nil, errors.New("needle " + fileId + " was vacuumed")
	}, cache, []*filer_pb.FileChunk{oldChunk}, 1, nil, nil)
	if err != nil {
		t.Fatalf("NewChunkGroup: %v", err)
	}
	fh.entryChunkGroup = group

	// Sanity check: reading before flush fails because chunkGroup points to oldChunk.
	preBuf := make([]byte, 16)
	if _, _, err := fh.entryChunkGroup.ReadDataAt(context.Background(), 16, preBuf, 0); err == nil || errors.Is(err, io.EOF) {
		t.Fatal("expected read against stale vacuumed oldChunk to fail before flush")
	}

	fh.dirtyMetadata = true
	if err := w.flushMetadataToFiler(context.Background(), fh, "/dir", "file", 0, 0, nil); err != nil {
		t.Fatalf("flushMetadataToFiler: %v", err)
	}

	// Entry chunks must have been compacted down to [newChunk].
	gotChunks := fh.GetEntry().GetChunks()
	if len(gotChunks) != 1 || contentFileIdKey(gotChunks[0]) != contentFileIdKey(newChunk) {
		t.Fatalf("unexpected entry chunks after flush: %v", gotChunks)
	}

	// And fh.entryChunkGroup must have been rebuilt to [newChunk], making reads succeed.
	gotBuf := make([]byte, 16)
	n, _, err := fh.entryChunkGroup.ReadDataAt(context.Background(), 16, gotBuf, 0)
	if err != nil && !errors.Is(err, io.EOF) {
		t.Fatalf("read after compacted flush failed (stale chunkGroup): %v", err)
	}
	if !bytes.Equal(gotBuf[:n], wantPayload) {
		t.Fatalf("read %q after compacted flush, want %q", gotBuf[:n], wantPayload)
	}
}

// PageWriter.ReadDirtyDataAt loops over chunks. If an earlier chunk has dirty
// bytes and a trailing chunk is clean (returning 0), maxStop must preserve the
// highest dirty offset rather than being clobbered back to 0.
func TestPageWriterReadDirtyDataAtAcrossDirtyAndCleanChunks(t *testing.T) {
	wfs := newInvalidateTestWFS(t)
	wfs.option.ChunkSizeLimit = 4096

	entry := &filer_pb.Entry{
		Name: "dirty_span",
		Attributes: &filer_pb.FuseAttributes{
			Inode:    99,
			FileMode: 0644,
		},
	}
	fh := newFileHandle(wfs, 1, 99, entry)
	defer fh.dirtyPages.Destroy()

	pw := fh.dirtyPages

	dirtyData := bytes.Repeat([]byte{'Z'}, 1024)
	tsNs := time.Now().UnixNano()
	if _, err := pw.AddPage(0, dirtyData, true, tsNs); err != nil {
		t.Fatalf("AddPage: %v", err)
	}

	// Read across chunk 0 [0, 4096) (dirty [0, 1024)) and chunk 1 [4096, 6144) (clean).
	readBuf := make([]byte, 6144)
	maxStop := pw.ReadDirtyDataAt(readBuf, 0, tsNs)
	if maxStop != 1024 {
		t.Fatalf("ReadDirtyDataAt maxStop = %d, want 1024 (trailing clean chunk clobbered maxStop)", maxStop)
	}
	if !bytes.Equal(readBuf[:1024], dirtyData) {
		t.Fatalf("dirty prefix mismatch in cross-chunk ReadDirtyDataAt")
	}
}
