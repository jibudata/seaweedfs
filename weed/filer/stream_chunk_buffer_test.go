package filer

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Interleaved ReadAt calls across chunks must not refetch a chunk every time
// the read window moves: parallel consumers (e.g. multipart upload parts read
// through io.SectionReader) share the reader and would otherwise keep evicting
// one another's single buffer, refetching whole chunks on nearly every read.
func TestChunkStreamReaderReadAtReusesFetchedChunks(t *testing.T) {
	contentA := bytes.Repeat([]byte("a"), 4096)
	contentB := bytes.Repeat([]byte("b"), 4096)
	var fetches atomic.Int32
	volume := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fetches.Add(1)
		switch strings.TrimPrefix(r.URL.Path, "/") {
		case "1,aaaaaaaa":
			_, _ = w.Write(contentA)
		case "2,bbbbbbbb":
			_, _ = w.Write(contentB)
		}
	}))
	t.Cleanup(volume.Close)

	lookup := func(ctx context.Context, fileId string) ([]string, error) {
		return []string{volume.URL + "/" + fileId}, nil
	}
	chunks := []*filer_pb.FileChunk{
		{FileId: "1,aaaaaaaa", Offset: 0, Size: uint64(len(contentA))},
		{FileId: "2,bbbbbbbb", Offset: int64(len(contentA)), Size: uint64(len(contentB))},
	}
	reader := NewChunkStreamReaderFromLookup(context.Background(), lookup, chunks)
	t.Cleanup(func() { _ = reader.Close() })

	buf := make([]byte, 64)
	for i := 0; i < 4; i++ {
		for _, off := range []int64{0, int64(len(contentA))} {
			_, err := reader.ReadAt(buf, off)
			require.NoError(t, err)
		}
	}
	assert.Equal(t, int32(2), fetches.Load(), "each chunk is fetched once, not once per read")

	_, err := reader.ReadAt(buf, 8)
	require.NoError(t, err)
	assert.Equal(t, contentA[8:72], buf)
}

func TestChunkStreamReaderSparseHoles(t *testing.T) {
	contentA := bytes.Repeat([]byte("a"), 1024)
	contentB := bytes.Repeat([]byte("b"), 1024)
	volume := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch strings.TrimPrefix(r.URL.Path, "/") {
		case "1,aaaaaaaa":
			_, _ = w.Write(contentA)
		case "2,bbbbbbbb":
			_, _ = w.Write(contentB)
		}
	}))
	t.Cleanup(volume.Close)

	lookup := func(ctx context.Context, fileId string) ([]string, error) {
		return []string{volume.URL + "/" + fileId}, nil
	}
	// Layout:
	// [0, 1024): sparse hole
	// [1024, 2048): chunk A
	// [2048, 4096): sparse hole
	// [4096, 5120): chunk B
	chunks := []*filer_pb.FileChunk{
		{FileId: "1,aaaaaaaa", Offset: 1024, Size: uint64(len(contentA)), ModifiedTsNs: 1},
		{FileId: "2,bbbbbbbb", Offset: 4096, Size: uint64(len(contentB)), ModifiedTsNs: 2},
	}

	want := make([]byte, 5120)
	copy(want[1024:2048], contentA)
	copy(want[4096:5120], contentB)

	reader := NewChunkStreamReaderFromLookup(context.Background(), lookup, chunks)
	t.Cleanup(func() { _ = reader.Close() })

	endOff, err := reader.Seek(0, io.SeekEnd)
	require.NoError(t, err)
	assert.Equal(t, int64(5120), endOff)

	_, err = reader.Seek(0, io.SeekStart)
	require.NoError(t, err)

	got, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, want, got)

	// Random ReadAt across holes and chunks in both forward and backward order.
	for _, window := range []struct {
		off  int64
		size int
	}{
		{4000, 256}, // hole -> chunk B boundary
		{0, 128},    // leading hole
		{1000, 128}, // leading hole -> chunk A boundary
		{2000, 128}, // chunk A -> middle hole boundary
		{2500, 512}, // inside middle hole
		{5000, 120}, // tail of chunk B
	} {
		buf := make([]byte, window.size)
		n, err := reader.ReadAt(buf, window.off)
		require.NoError(t, err)
		assert.Equal(t, window.size, n)
		assert.Equal(t, want[window.off:window.off+int64(window.size)], buf)
	}
}

