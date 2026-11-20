package weed_server

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/storage"
	"github.com/seaweedfs/seaweedfs/weed/storage/backend"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/stretchr/testify/require"
)

// heldSyncBackend parks the first .dat fsync until released: the window where
// a durable write is appended but not indexed yet.
type heldSyncBackend struct {
	backend.BackendStorageFile
	once    sync.Once
	entered chan struct{}
	release chan struct{}
}

func (b *heldSyncBackend) Sync() error {
	b.once.Do(func() {
		close(b.entered)
		<-b.release
	})
	return b.BackendStorageFile.Sync()
}

// startHeldWrite starts a durable write of needle 1 on a fresh volume and
// returns once it is parked in its .dat fsync, appended but not indexed.
func startHeldWrite(t *testing.T) (store *storage.Store, fid string, release func(), written <-chan error) {
	store = storage.NewStore(nil, "localhost", 8080, 18080, "http://localhost:8080", "store-id",
		[]string{t.TempDir()}, []int32{10}, []util.MinFreeSpace{{}}, "", storage.NeedleMapInMemory,
		[]types.DiskType{types.HardDriveType}, nil, 0, stats.DefaultDiskIOProbeConfig())
	t.Cleanup(store.Close)
	require.NoError(t, store.AddVolume(1, "", storage.NeedleMapInMemory, "000", "", 0, needle.Version3, 0, types.HardDriveType, 0))
	v := store.GetVolume(1)
	held := &heldSyncBackend{BackendStorageFile: v.DataBackend, entered: make(chan struct{}), release: make(chan struct{})}
	v.DataBackend = held

	n := &needle.Needle{Id: 1, Cookie: 0x1234, Data: []byte("late write")}
	n.Checksum = needle.NewCRC(n.Data)
	done := make(chan error, 1)
	go func() {
		_, err := store.WriteVolumeNeedle(1, n, true, true)
		done <- err
	}()
	<-held.entered
	var releaseOnce sync.Once
	release = func() { releaseOnce.Do(func() { close(held.release) }) }
	t.Cleanup(release)
	return store, needle.NewFileIdFromNeedle(1, n).String(), release, done
}

func requireTakenBack(t *testing.T, store *storage.Store, written <-chan error, takenBack bool) {
	t.Helper()
	err := <-written
	_, readErr := store.ReadVolumeNeedle(1, &needle.Needle{Id: 1}, nil, nil)
	if takenBack {
		require.ErrorIs(t, err, storage.ErrorDeleted)
		require.ErrorIs(t, readErr, storage.ErrorDeleted)
	} else {
		require.NoError(t, err)
		require.NoError(t, readErr)
	}
}

// A replica delete that arrives while the write it undoes is in its fsync
// finds nothing, yet the write must not land live afterwards. A plain delete
// on an unreplicated volume that finds nothing leaves the write.
func TestDeleteHandlerCancelsAWriteInItsFsyncWindow(t *testing.T) {
	for _, tc := range []struct {
		query string
		live  bool
	}{
		{"?type=replicate", false},
		{"", true},
	} {
		t.Run(tc.query, func(t *testing.T) {
			store, fid, release, written := startHeldWrite(t)
			resp := httptest.NewRecorder()
			vs := &VolumeServer{store: store, guard: security.NewGuard(nil, "", 0, "", 0)}
			vs.DeleteHandler(resp, httptest.NewRequest(http.MethodDelete, "/"+fid+tc.query, nil))
			require.Equal(t, http.StatusNotFound, resp.Code)
			release()
			requireTakenBack(t, store, written, !tc.live)
		})
	}
}

// failingReadBackend fails every read while fail is set.
type failingReadBackend struct {
	backend.BackendStorageFile
	fail atomic.Bool
}

func (b *failingReadBackend) ReadAt(p []byte, off int64) (int, error) {
	if b.fail.Load() {
		return 0, errors.New("injected read error")
	}
	return b.BackendStorageFile.ReadAt(p, off)
}

// A delete whose fan-out failed was already applied here, so the client's
// retry finds nothing locally; it must still reach the replica that kept the
// needle, or the replicas stay different. The replica either refuses the
// first forward outright or cannot read the needle, which is not "gone".
func TestDeleteHandlerRetryReachesTheReplicaAfterAFailedFanOut(t *testing.T) {
	for i, replicaFails := range []string{"refuses", "cannot read"} {
		t.Run(replicaFails, func(t *testing.T) {
			vid := needle.VolumeId(849300202 + i)
			allocate := func(vs *VolumeServer) {
				vs.store = newTraversalTestStore(t.TempDir())
				t.Cleanup(vs.store.Close)
				_, err := vs.AllocateVolume(context.Background(), &volume_server_pb.AllocateVolumeRequest{VolumeId: uint32(vid), Replication: "001", Version: uint32(needle.GetCurrentVersion())})
				require.NoError(t, err)
				vs.guard = security.NewGuard(nil, "", 0, "", 0)
			}
			replicaVs := &VolumeServer{}
			allocate(replicaVs)
			var refuseNext atomic.Bool
			replica := newProxyTestHTTPServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if refuseNext.CompareAndSwap(true, false) {
					w.WriteHeader(http.StatusInternalServerError)
					return
				}
				replicaVs.DeleteHandler(w, r)
			}))

			primary := &master_pb.Location{}
			vs := newProxyTestVolumeServer(t, vid.String(), []*master_pb.Location{primary, {Url: strings.TrimPrefix(replica.URL, "http://")}}, nil)
			allocate(vs)
			primary.Url = util.JoinHostPort(vs.store.Ip, vs.store.Port)

			fid := needle.NewFileId(vid, 1, 0x1234)
			for _, s := range []*storage.Store{vs.store, replicaVs.store} {
				n := &needle.Needle{Id: fid.Key, Cookie: fid.Cookie, Data: []byte("replicated delete")}
				n.Checksum = needle.NewCRC(n.Data)
				_, err := s.WriteVolumeNeedle(vid, n, true, false)
				require.NoError(t, err)
			}
			replicaVolume := replicaVs.store.GetVolume(vid)
			reads := &failingReadBackend{BackendStorageFile: replicaVolume.DataBackend}
			replicaVolume.DataBackend = reads
			live := func(s *storage.Store) bool {
				_, err := s.ReadVolumeNeedle(vid, &needle.Needle{Id: fid.Key, Cookie: fid.Cookie}, nil, nil)
				return err == nil
			}

			if replicaFails == "refuses" {
				refuseNext.Store(true)
			} else {
				reads.fail.Store(true)
			}
			resp := httptest.NewRecorder()
			vs.DeleteHandler(resp, httptest.NewRequest(http.MethodDelete, "/"+fid.String(), nil))
			require.Equal(t, http.StatusInternalServerError, resp.Code, resp.Body.String())
			reads.fail.Store(false)
			require.False(t, live(vs.store))
			require.True(t, live(replicaVs.store))

			resp = httptest.NewRecorder()
			vs.DeleteHandler(resp, httptest.NewRequest(http.MethodDelete, "/"+fid.String(), nil))
			require.Equal(t, http.StatusNotFound, resp.Code, resp.Body.String())
			require.False(t, live(replicaVs.store))
		})
	}
}

// A batch delete that skips the cookie check still cancels the in-flight
// writes of the file id it names, and only those.
func TestBatchDeleteCancelsAWriteInItsFsyncWindow(t *testing.T) {
	for _, tc := range []struct {
		cookie    string
		takenBack bool
	}{
		{"", true},
		{"00004321", false},
	} {
		t.Run(tc.cookie, func(t *testing.T) {
			store, fid, release, written := startHeldWrite(t)
			if tc.cookie != "" {
				fid = fid[:len(fid)-8] + tc.cookie
			}
			deleted := make(chan error, 1)
			go func() {
				_, err := (&VolumeServer{store: store}).BatchDelete(context.Background(), &volume_server_pb.BatchDeleteRequest{
					FileIds:         []string{fid},
					SkipCookieCheck: true,
				})
				deleted <- err
			}()
			// the delete waits behind the held write's sync lock
			require.Eventually(t, func() bool {
				buf := make([]byte, 1<<20)
				return bytes.Contains(buf[:runtime.Stack(buf, true)], []byte("(*Volume).syncDelete"))
			}, 10*time.Second, time.Millisecond)
			release()
			require.NoError(t, <-deleted)
			err := <-written
			if tc.takenBack {
				require.ErrorIs(t, err, storage.ErrorDeleted)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
