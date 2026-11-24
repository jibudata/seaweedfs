package weed_server

import (
	"bytes"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/storage"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// A HEAD only attempts a metadata-only read, like a GET: a needle up to
// PagedReadLimit is read whole. Demanding metadata only would turn that one
// read into a header read followed by a trailer read. A ranged GET skips the
// payload at any size, since it streams only the range.
func TestShouldAttemptStreamWrite(t *testing.T) {
	for _, tc := range []struct {
		name         string
		method       string
		target       string
		ext          string
		rangeHeader  string
		hasVolume    bool
		wantAttempt  bool
		wantMustMeta bool
		wantRange    bool
	}{
		{"HEAD", http.MethodHead, "/1,abc.jpg", ".jpg", "", true, true, false, false},
		{"GET", http.MethodGet, "/1,abc.jpg", ".jpg", "", true, true, false, false},
		{"ranged GET", http.MethodGet, "/1,abc", "", "bytes=0-99", true, true, false, true},
		{"ranged HEAD", http.MethodHead, "/1,abc", "", "bytes=0-99", true, true, false, false},
		{"ranged GET of a resized image", http.MethodGet, "/1,abc.jpg?width=100", ".jpg", "bytes=0-99", true, false, false, false},
		{"ranged GET to resize, no extension", http.MethodGet, "/1,abc?width=100", "", "bytes=0-99", true, true, false, false},
		{"ranged GET to crop, no extension", http.MethodGet, "/1,abc?crop_x1=0&crop_y1=0&crop_x2=10&crop_y2=10", "", "bytes=0-99", true, true, false, false},
		{"ranged GET with a width on a non-image", http.MethodGet, "/1,abc.bin?width=100", ".bin", "bytes=0-99", true, true, false, true},
		{"ranged GET without a local volume", http.MethodGet, "/1,abc", "", "bytes=0-99", false, false, false, false},
		{"GET of a resized image", http.MethodGet, "/1,abc.jpg?width=100", ".jpg", "", true, false, false, false},
		{"HEAD of a resized image", http.MethodHead, "/1,abc.jpg?width=100", ".jpg", "", true, false, false, false},
		{"HEAD of a cropped image", http.MethodHead, "/1,abc.jpg?crop_x1=0&crop_y1=0&crop_x2=100&crop_y2=100", ".jpg", "", true, false, false, false},
		{"HEAD without a local volume", http.MethodHead, "/1,abc.jpg", ".jpg", "", false, false, false, false},
	} {
		r := httptest.NewRequest(tc.method, tc.target, nil)
		if tc.rangeHeader != "" {
			r.Header.Set("Range", tc.rangeHeader)
		}
		attempt, mustMeta, hasRange := shouldAttemptStreamWrite(tc.hasVolume, tc.ext, r)
		if attempt != tc.wantAttempt || mustMeta != tc.wantMustMeta || hasRange != tc.wantRange {
			t.Errorf("%s: attempt %v, must be metadata only %v, range %v; want %v, %v, %v",
				tc.name, attempt, mustMeta, hasRange, tc.wantAttempt, tc.wantMustMeta, tc.wantRange)
		}
	}
}

func TestGetOrHeadHandlerRangeOnCompressedNeedleServesDecodedBytes(t *testing.T) {
	dir := t.TempDir()
	store := storage.NewStore(nil, "localhost", 8080, 18080, "http://localhost:8080", "store-id",
		[]string{dir}, []int32{10}, []util.MinFreeSpace{{}}, "",
		storage.NeedleMapInMemory, []types.DiskType{types.HardDriveType}, nil, 3, stats.DefaultDiskIOProbeConfig())
	done := make(chan struct{})
	go func() {
		for {
			select {
			case <-store.NewVolumesChan:
			case <-store.DeletedVolumesChan:
			case <-store.NewEcShardsChan:
			case <-store.DeletedEcShardsChan:
			case <-store.StateUpdateChan:
			case <-done:
				return
			}
		}
	}()
	t.Cleanup(func() {
		store.Close()
		close(done)
	})
	if err := store.AddVolume(1, "", storage.NeedleMapInMemory, "000", "", 0, needle.GetCurrentVersion(), 0, types.HardDriveType, 0); err != nil {
		t.Fatalf("add volume: %v", err)
	}

	plain := []byte(strings.Repeat("the quick brown fox jumps over the lazy dog\n", 200))
	compressed, err := util.GzipData(plain)
	if err != nil {
		t.Fatalf("gzip: %v", err)
	}
	n := &needle.Needle{Id: 1, Cookie: 0x1234, Data: compressed}
	n.SetIsCompressed()
	n.Checksum = needle.NewCRC(n.Data)
	if _, err := store.WriteVolumeNeedle(1, n, true, false); err != nil {
		t.Fatalf("write needle: %v", err)
	}
	vs := &VolumeServer{store: store, ReadMode: "local", guard: security.NewGuard(nil, "", 0, "", 0)}

	serve := func(method, rangeHeader string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(method, "/1,0100001234", nil)
		r.Header.Set("Accept-Encoding", "gzip")
		if rangeHeader != "" {
			r.Header.Set("Range", rangeHeader)
		}
		w := httptest.NewRecorder()
		vs.GetOrHeadHandler(w, r)
		return w
	}

	// A range is over the decoded bytes, even past the compressed size.
	start, end := len(compressed)+10, len(compressed)+69
	w := serve(http.MethodGet, fmt.Sprintf("bytes=%d-%d", start, end))
	if w.Code != http.StatusPartialContent {
		t.Fatalf("ranged GET status %d, want 206", w.Code)
	}
	if ce := w.Header().Get("Content-Encoding"); ce != "" {
		t.Fatalf("ranged GET Content-Encoding %q, want none", ce)
	}
	if want := fmt.Sprintf("bytes %d-%d/%d", start, end, len(plain)); w.Header().Get("Content-Range") != want {
		t.Fatalf("ranged GET Content-Range %q, want %q", w.Header().Get("Content-Range"), want)
	}
	if got := w.Body.String(); got != string(plain[start:end+1]) {
		t.Fatalf("ranged GET body %q, want %q", got, plain[start:end+1])
	}

	w = serve(http.MethodHead, "bytes=0-9")
	if ce := w.Header().Get("Content-Encoding"); ce != "" {
		t.Fatalf("ranged HEAD Content-Encoding %q, want none", ce)
	}
	if cl := w.Header().Get("Content-Length"); cl != strconv.Itoa(len(plain)) {
		t.Fatalf("ranged HEAD Content-Length %s, want %d", cl, len(plain))
	}

	// A whole read still passes the stored gzip through.
	w = serve(http.MethodGet, "")
	if w.Code != http.StatusOK || w.Header().Get("Content-Encoding") != "gzip" {
		t.Fatalf("whole GET status %d Content-Encoding %q, want 200 gzip", w.Code, w.Header().Get("Content-Encoding"))
	}
	if !bytes.Equal(w.Body.Bytes(), compressed) {
		t.Fatalf("whole GET body is not the stored gzip")
	}
}
