package weed_server

import (
	"net/http"
	"net/http/httptest"
	"testing"
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
