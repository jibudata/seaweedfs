package weed_server

import (
	"net/http"
	"path/filepath"
	"time"

	"github.com/chrislusf/seaweedfs/weed/pb"
	"github.com/chrislusf/seaweedfs/weed/pb/volume_server_pb"
	ui "github.com/chrislusf/seaweedfs/weed/server/volume_server_ui"
	"github.com/chrislusf/seaweedfs/weed/stats"
	"github.com/chrislusf/seaweedfs/weed/storage"
	"github.com/chrislusf/seaweedfs/weed/storage/backend"
	"github.com/chrislusf/seaweedfs/weed/util"
)

func (vs *VolumeServer) uiStatusHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Server", "SeaweedFS Volume "+util.VERSION)
	infos := make(map[string]interface{})
	infos["Up Time"] = time.Now().Sub(startTime).String()
	var ds []*volume_server_pb.DiskStatus
	for _, loc := range vs.store.Locations {
		if dir, e := filepath.Abs(loc.Directory); e == nil {
			newDiskStatus := stats.NewDiskStatus(dir)
			newDiskStatus.DiskType = loc.DiskType.String()
			ds = append(ds, newDiskStatus)
		}
	}
	volumeInfos := vs.store.VolumeInfos()
	var normalVolumeInfos, remoteVolumeInfos []*storage.VolumeInfo
	for _, vinfo := range volumeInfos {
		if vinfo.IsRemote() {
			remoteVolumeInfos = append(remoteVolumeInfos, vinfo)
		} else {
			normalVolumeInfos = append(normalVolumeInfos, vinfo)
		}
	}

	var bs []*volume_server_pb.BackendStatus
	if dm, exists := backend.BackendStorages["ltfsdm"]; exists {
		root_path := dm.ToProperties()["root_path"]
		dmStatus := stats.NewBackendStatus(root_path)
		dmStatus.BackendType = "ltfsdm"
		bs = append(bs, dmStatus)
	}
	args := struct {
		Version         string
		Masters         []pb.ServerAddress
		Volumes         interface{}
		EcVolumes       interface{}
		RemoteVolumes   interface{}
		DiskStatuses    interface{}
		BackendStatuses interface{}
		Stats           interface{}
		Counters        *stats.ServerStats
	}{
		util.Version(),
		vs.SeedMasterNodes,
		normalVolumeInfos,
		vs.store.EcVolumes(),
		remoteVolumeInfos,
		ds,
		bs,
		infos,
		serverStats,
	}
	ui.StatusTpl.Execute(w, args)
}
