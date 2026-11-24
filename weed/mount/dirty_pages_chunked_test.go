package mount

import (
	"errors"
	"io"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

type flakySave struct {
	failures []error
	calls    int
	bodies   []string
	onCall   func()
}

func (s *flakySave) saveDataAsChunk(util.FullPath) filer.SaveDataAsChunkFunctionType {
	return func(reader io.Reader, name string, offset int64, tsNs int64, _ uint64) (*filer_pb.FileChunk, error) {
		body, _ := io.ReadAll(reader)
		s.bodies = append(s.bodies, string(body))
		s.calls++
		if s.onCall != nil {
			s.onCall()
		}
		if s.calls <= len(s.failures) {
			return nil, s.failures[s.calls-1]
		}
		return &filer_pb.FileChunk{FileId: "1,01", Offset: offset, Size: uint64(len(body)), ModifiedTsNs: tsNs}, nil
	}
}

// Once a chunk upload fails its data is gone, so the handle must reject
// further writes instead of buffering the rest of the file — against a full
// cluster that turned cp into an hours-long crawl that only errored at close.
func TestChunkedDirtyPagesFailWritesAfterUploadError(t *testing.T) {
	fh := &FileHandle{wfs: &WFS{option: &Option{}}}
	pages := newMemoryChunkPages(fh, 1024)
	defer pages.Destroy()
	pages.hasWrites = true

	uploadErr := errors.New("assign volume failure: no writable volumes")
	pages.setLastError(uploadErr)

	if _, err := pages.AddPage(0, []byte("x"), true, 1); !errors.Is(err, uploadErr) {
		t.Fatalf("AddPage after upload failure = %v, want sticky %v", err, uploadErr)
	}
	if err := pages.FlushData(); !errors.Is(err, uploadErr) {
		t.Fatalf("FlushData after upload failure = %v, want wrapped %v", err, uploadErr)
	}
	// First failure wins; later errors must not mask the root cause.
	pages.setLastError(errors.New("later error"))
	if err := pages.LastError(); !errors.Is(err, uploadErr) {
		t.Fatalf("LastError = %v, want first error %v", err, uploadErr)
	}
}
