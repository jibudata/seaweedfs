package mount

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

// A file id is "volume,key,cookie": the key is 8 bytes and the cookie 4,
// both hex, so a full id carries 24 hex characters.
func mkChunk(id string, off int64) *filer_pb.FileChunk {
	return &filer_pb.FileChunk{FileId: id, Offset: off, Size: 10}
}

const (
	chunkA = "1,000000000000000100000000"
	chunkB = "1,000000000000000200000000"
	chunkC = "1,000000000000000300000000"
)

// A merge replaces the file's chunk list; the chunks it replaced are the
// ones no list names any more, and only those may be deleted.
func TestReplacedChunks(t *testing.T) {
	a, b, c := mkChunk(chunkA, 0), mkChunk(chunkB, 10), mkChunk(chunkC, 20)

	// the new list names b and c: only a was replaced
	replaced := replacedChunks([]*filer_pb.FileChunk{a, b, c}, []*filer_pb.FileChunk{b, c})
	if len(replaced) != 1 || replaced[0] != a {
		t.Fatalf("expected only the replaced chunk, got %v", replaced)
	}

	// the new list names everything: nothing was replaced
	replaced = replacedChunks([]*filer_pb.FileChunk{a, b}, []*filer_pb.FileChunk{a, b})
	if len(replaced) != 0 {
		t.Fatalf("expected nothing replaced, got %v", replaced)
	}

	// a chunk without an id cannot be deleted, so it is not a candidate
	replaced = replacedChunks([]*filer_pb.FileChunk{{Offset: 0, Size: 1}}, []*filer_pb.FileChunk{b})
	if len(replaced) != 0 {
		t.Fatalf("expected the id-less chunk to be skipped, got %v", replaced)
	}
}

// The same needle named by both lists is kept: the id, not the offset, is
// what the volume server stores.
func TestReplacedChunksComparesById(t *testing.T) {
	same := mkChunk(chunkA, 0)
	moved := mkChunk(chunkA, 4096)
	other := mkChunk(chunkB, 0)

	replaced := replacedChunks([]*filer_pb.FileChunk{same, other}, []*filer_pb.FileChunk{moved})
	if len(replaced) != 1 || replaced[0] != other {
		t.Fatalf("expected only the other chunk to be replaced, got %v", replaced)
	}
}

// The volume server's batch delete parses "volume,key,cookie"; a chunk that
// carries only the short "volume,key" form must be expanded, or the delete
// would address the wrong needle.
func TestChunkNeedleId(t *testing.T) {
	short := &filer_pb.FileChunk{FileId: "3,01637037d601234500000000"}
	id := chunkNeedleId(short)
	if id == "" {
		t.Fatal("expected an id for a short form chunk")
	}
	if len(id) <= len("3,")+8 {
		t.Fatalf("expected a key and a cookie in %q", id)
	}

	withFid := &filer_pb.FileChunk{
		FileId: "3,01637037d601234500000000",
		Fid:    &filer_pb.FileId{VolumeId: 3, FileKey: 0x01637037d6012345, Cookie: 0},
	}
	if got := chunkNeedleId(withFid); got != id {
		t.Fatalf("expected the fid form to match the parsed form: %q vs %q", got, id)
	}

	if got := chunkNeedleId(nil); got != "" {
		t.Fatalf("expected no id for a nil chunk, got %q", got)
	}
}
