package mount

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

// A merge that read less than the file would publish a rewrite that does
// not cover the whole file, so the guard must refuse it.
func TestMergeReadIsComplete(t *testing.T) {
	const size = 4 << 30
	cases := []struct {
		name     string
		offset   int64
		fileSize uint64
		complete bool
	}{
		{"nothing read", 0, size, false},
		{"one MiB of a four GiB file", 1 << 20, size, false},
		{"one byte short", size - 1, size, false},
		{"the whole file", size, size, true},
	}
	for _, c := range cases {
		if got := mergeReadIsComplete(c.offset, c.fileSize); got != c.complete {
			t.Errorf("%s: got %v want %v", c.name, got, c.complete)
		}
	}
}

// A flush must keep the chunks that upload completions appended while it
// ran: the processed list replaces the snapshot, and a chunk attached
// after the snapshot was taken is not part of the merge and stays.
func TestWithChunksAttachedSince(t *testing.T) {
	mk := func(id string, off int64) *filer_pb.FileChunk {
		return &filer_pb.FileChunk{FileId: id, Offset: off, Size: 10}
	}
	a, b, c := mk("1,a", 0), mk("1,b", 10), mk("1,c", 20)

	got := withChunksAttachedSince(
		[]*filer_pb.FileChunk{a, b},
		[]*filer_pb.FileChunk{b},
		[]*filer_pb.FileChunk{a, b, c})
	if len(got) != 2 || got[0] != b || got[1] != c {
		t.Fatalf("expected the processed chunk then the attached one, got %v", got)
	}

	got = withChunksAttachedSince(
		[]*filer_pb.FileChunk{a, b},
		[]*filer_pb.FileChunk{b},
		[]*filer_pb.FileChunk{a, b})
	if len(got) != 1 || got[0] != b {
		t.Fatalf("expected only the processed chunk, got %v", got)
	}
}
