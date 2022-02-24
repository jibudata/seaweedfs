package stats

import (
	"syscall"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/volume_server_pb"
	pb "github.com/chrislusf/seaweedfs/weed/storage/backend/archival_backend/archival_api"
)

func NewBackendStatus(path string) (backend *volume_server_pb.BackendStatus) {
	backend = &volume_server_pb.BackendStatus{Dir: path}
	fillInBackendStatus(backend)
	if backend.PercentUsed > 95 {
		glog.V(0).Infof("backend status: %v", backend)
	}
	return
}

func fillInBackendStatus(backend *volume_server_pb.BackendStatus) {
	fs := syscall.Statfs_t{}
	err := syscall.Statfs(backend.Dir, &fs)
	if err != nil {
		return
	}
	backend.All = fs.Blocks * uint64(fs.Bsize)
	backend.Free = fs.Bfree * uint64(fs.Bsize)
	backend.Used = backend.All - backend.Free
	backend.PercentFree = float32((float64(backend.Free) / float64(backend.All)) * 100)
	backend.PercentUsed = float32((float64(backend.Used) / float64(backend.All)) * 100)
	return
}

func NewArchivalBackendStatus(addr string, pool string) (backend *volume_server_pb.BackendStatus) {
	backend = &volume_server_pb.BackendStatus{Dir: addr + "/" + pool}
	front, err := pb.NewFrontApi(addr)
	if err != nil {
		glog.V(1).Infof("New Front Api failed")
		return
	} else {
		pool, found := front.GetPoolInfo(pool)
		if(found) {
			backend.All = pool.Total
			backend.Free = pool.Free
			backend.Used = backend.All - backend.Free
			backend.PercentFree = float32((float64(backend.Free) / float64(backend.All)) * 100)
			backend.PercentUsed = float32((float64(backend.Used) / float64(backend.All)) * 100)
		}
	}
	return
}