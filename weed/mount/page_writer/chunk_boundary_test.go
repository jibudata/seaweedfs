package page_writer

import (
	"bytes"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/util"
)

func TestSwapFileChunkWriteClampsToChunkBoundary(t *testing.T) {
	const chunkSize int64 = 4096
	sf := NewSwapFile(t.TempDir(), chunkSize)
	defer sf.FreeResource()

	c0 := sf.NewSwapFileChunk(0)
	if c0 == nil {
		t.Fatal("failed to create swap chunk 0")
	}
	c1 := sf.NewSwapFileChunk(1)
	if c1 == nil {
		t.Fatal("failed to create swap chunk 1")
	}

	// Seed chunk 1 with a known sentinel pattern.
	sentinel := bytes.Repeat([]byte{0x5A}, int(chunkSize))
	if n := c1.WriteDataAt(sentinel, chunkSize, 1); n != int(chunkSize) {
		t.Fatalf("seed chunk 1 wrote %d, want %d", n, chunkSize)
	}

	// Attempt an oversized write on chunk 0 starting 96 bytes before the chunk
	// boundary. Without clamping, the trailing 416 bytes overwrite chunk 1.
	payload := bytes.Repeat([]byte{0xA5}, 512)
	n := c0.WriteDataAt(payload, chunkSize-96, 2)
	if n != 96 {
		t.Fatalf("chunk 0 WriteDataAt returned %d, want clamped 96", n)
	}

	got0 := make([]byte, 96)
	if stop := c0.ReadDataAt(got0, chunkSize-96, 3); stop != chunkSize {
		t.Fatalf("chunk 0 ReadDataAt stop = %d, want %d", stop, chunkSize)
	}
	if !bytes.Equal(got0, payload[:96]) {
		t.Fatalf("chunk 0 data mismatch")
	}

	got1 := make([]byte, chunkSize)
	if stop := c1.ReadDataAt(got1, chunkSize, 3); stop != 2*chunkSize {
		t.Fatalf("chunk 1 ReadDataAt stop = %d, want %d", stop, 2*chunkSize)
	}
	if !bytes.Equal(got1, sentinel) {
		t.Fatalf("adjacent swap chunk 1 was corrupted by cross-boundary write on chunk 0")
	}
}

func TestUploadPipelineSaveAndReadAcrossChunkBoundaries(t *testing.T) {
	const chunkSize int64 = 4096

	for _, tc := range []struct {
		name             string
		bufferChunkLimit int
		isSequential     bool
	}{
		{name: "swapfile", bufferChunkLimit: 0, isSequential: false},
		{name: "memchunk", bufferChunkLimit: 8, isSequential: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var mu sync.Mutex
			uploaded := make([]byte, 16384)

			saveFn := func(reader io.Reader, offset int64, size int64, modifiedTsNs int64, cleanupFn func()) {
				defer cleanupFn()
				data, err := io.ReadAll(reader)
				if err != nil {
					t.Errorf("ReadAll: %v", err)
					return
				}
				mu.Lock()
				copy(uploaded[offset:offset+int64(len(data))], data)
				mu.Unlock()
			}

			up := NewUploadPipeline(
				util.NewLimitedConcurrentExecutor(2),
				chunkSize,
				saveFn,
				tc.bufferChunkLimit,
				t.TempDir(),
				nil,
			)
			defer up.Shutdown()

			// Write a 10,000-byte deterministic pattern starting at offset 3000,
			// spanning chunks 0, 1, and 2 in a single SaveDataAt call.
			const startOff int64 = 3000
			payload := make([]byte, 10000)
			for i := range payload {
				payload[i] = byte((i*31 + 7) % 251)
			}

			ts := time.Now().UnixNano()
			n, err := up.SaveDataAt(payload, startOff, tc.isSequential, ts)
			if err != nil {
				t.Fatalf("SaveDataAt error: %v", err)
			}
			if n != len(payload) {
				t.Fatalf("SaveDataAt wrote %d bytes, want %d", n, len(payload))
			}

			// Verify cross-chunk MaybeReadDataAt before flush.
			readBuf := make([]byte, len(payload))
			maxStop := up.MaybeReadDataAt(readBuf, startOff, ts)
			if maxStop != startOff+int64(len(payload)) {
				t.Fatalf("MaybeReadDataAt maxStop = %d, want %d", maxStop, startOff+int64(len(payload)))
			}
			if !bytes.Equal(readBuf, payload) {
				t.Fatalf("MaybeReadDataAt cross-chunk data mismatch")
			}

			// Verify uploaded bytes after FlushAll.
			up.FlushAll()
			mu.Lock()
			gotUploaded := append([]byte(nil), uploaded[startOff:startOff+int64(len(payload))]...)
			mu.Unlock()
			if !bytes.Equal(gotUploaded, payload) {
				t.Fatalf("uploaded cross-chunk data mismatch")
			}
		})
	}
}
