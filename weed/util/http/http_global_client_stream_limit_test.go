package http

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/util"
)

// TestReadUrlAsStreamCapsTruncatedFullChunk guards against stale truncated tail
// leakage when weed mount SetAttr(truncate) shrinks a chunk's metadata Size
// below the physical needle size stored on the volume server. Because
// ChunkView.IsFullChunk() compares ViewSize == ChunkSize (both equal to the
// truncated size), ReadUrlAsStream omits the Range header and must cap output
// to size bytes whenever size > 0.
func TestReadUrlAsStreamCapsTruncatedFullChunk(t *testing.T) {
	InitGlobalHttpClient()

	// Exceed the 256 KiB internal stream buffer so multi-read clamping is exercised.
	const physicalSize = 600*1024 + 31
	const truncatedSize = 300*1024 + 17
	payload := make([]byte, physicalSize)
	for i := range payload {
		payload[i] = byte((i*31 + 7) & 0xff)
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(payload)
	}))
	defer server.Close()

	t.Run("isFullChunk with positive size caps output to size", func(t *testing.T) {
		var got bytes.Buffer
		retryable, err := ReadUrlAsStream(context.Background(), server.URL, "", nil, false, true, 0, truncatedSize, func(data []byte) {
			got.Write(data)
		})
		if err != nil {
			t.Fatalf("ReadUrlAsStream failed (retryable=%v): %v", retryable, err)
		}
		if got.Len() != truncatedSize {
			t.Fatalf("expected %d bytes, got %d", truncatedSize, got.Len())
		}
		if !bytes.Equal(got.Bytes(), payload[:truncatedSize]) {
			t.Fatalf("streamed bytes mismatch expected truncated prefix")
		}
	})

	t.Run("isFullChunk with size=0 streams full body to EOF", func(t *testing.T) {
		var got bytes.Buffer
		retryable, err := ReadUrlAsStream(context.Background(), server.URL, "", nil, false, true, 0, 0, func(data []byte) {
			got.Write(data)
		})
		if err != nil {
			t.Fatalf("ReadUrlAsStream(size=0) failed (retryable=%v): %v", retryable, err)
		}
		if got.Len() != physicalSize {
			t.Fatalf("expected full %d bytes, got %d", physicalSize, got.Len())
		}
		if !bytes.Equal(got.Bytes(), payload) {
			t.Fatalf("streamed bytes mismatch full payload")
		}
	})
}

func TestReadUrlAsStreamEncryptedCapsTruncatedFullChunk(t *testing.T) {
	InitGlobalHttpClient()

	key := util.GenCipherKey()
	payload := bytes.Repeat([]byte("truncated-encrypted-chunk-"), 256)
	truncatedSize := 1024 + 19

	encrypted, err := util.Encrypt(payload, key)
	if err != nil {
		t.Fatalf("util.Encrypt failed: %v", err)
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(encrypted)
	}))
	defer server.Close()

	t.Run("encrypted isFullChunk with positive size caps to size", func(t *testing.T) {
		var got bytes.Buffer
		_, err := ReadUrlAsStream(context.Background(), server.URL, "", []byte(key), false, true, 0, truncatedSize, func(data []byte) {
			got.Write(data)
		})
		if err != nil {
			t.Fatalf("ReadUrlAsStream(encrypted, size>0) failed: %v", err)
		}
		if got.Len() != truncatedSize || !bytes.Equal(got.Bytes(), payload[:truncatedSize]) {
			t.Fatalf("expected %d-byte truncated prefix, got %d bytes", truncatedSize, got.Len())
		}
	})

	t.Run("encrypted isFullChunk with size=0 returns full payload", func(t *testing.T) {
		var got bytes.Buffer
		_, err := ReadUrlAsStream(context.Background(), server.URL, "", []byte(key), false, true, 0, 0, func(data []byte) {
			got.Write(data)
		})
		if err != nil {
			t.Fatalf("ReadUrlAsStream(encrypted, size=0) failed: %v", err)
		}
		if got.Len() != len(payload) || !bytes.Equal(got.Bytes(), payload) {
			t.Fatalf("expected full %d bytes, got %d bytes", len(payload), got.Len())
		}
	})
}
