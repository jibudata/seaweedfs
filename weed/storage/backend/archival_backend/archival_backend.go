package archival_backend

import (
	"fmt"
	"os"
	"time"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/volume_server_pb"
	"github.com/chrislusf/seaweedfs/weed/storage/backend"
	pb "github.com/chrislusf/seaweedfs/weed/storage/backend/archival_backend/archival_api"
	"github.com/golang/protobuf/proto"
	"github.com/google/uuid"
)

func init() {
	backend.BackendStorageFactories["archival"] = &ArchivalBackendFactory{}
}

// LTFSDM status
const (
	archivalStateRunning    = "running"
	archivalStateNotManaged = "unmanaged"
	archivalStateDown       = "down"
)

type ArchivalBackendFactory struct {
}

func (f *ArchivalBackendFactory) StorageType() backend.StorageType {
	return backend.StorageType("archival")
}

func (f *ArchivalBackendFactory) BuildStorage(conf backend.StringProperties, configPrefix string, id string) (backend.BackendStorage, error) {
	return newArchivalBackendStorage(conf, configPrefix, id)
}

type ArchivalBackendStorage struct {
	id              string
	addr            string
	pool            string
	managedFsStatus string
	remoteInfo      string
}

func newArchivalBackendStorage(conf backend.StringProperties, configPrefix string, id string) (s *ArchivalBackendStorage, err error) {

	f := &ArchivalBackendStorage{
		id:   id,
		addr: conf.GetString(configPrefix + "addr"),
		pool: conf.GetString(configPrefix + "pool"),
	}
	glog.V(0).Infof("create archival backend storage,id: archival.%s, root path: %s", id, f.addr)

	return f, nil
}

// ArchivalBackendStorage Implement BackendStorage interface
func (s *ArchivalBackendStorage) ToProperties() map[string]string {
	m := make(map[string]string)
	m["addr"] = s.addr
	m["pool"] = s.pool
	return m
}

func (s *ArchivalBackendStorage) NewStorageFile(volFileName, key string, tierInfo *volume_server_pb.VolumeInfo) backend.BackendStorageFile {
	// Open file from fuse mount point
	destFilename := volFileName + ".dat"
	destFile := destFilename

	return &ArchivalBackendStorageFile{
		volDataFile: volFileName,
		destFile:    destFile,
		backend:     s,
		tierInfo:    tierInfo,
	}
}

func (s *ArchivalBackendStorage) CopyFile(fullpath string, f *os.File, fn func(progressed int64, percentage float32) error) (key string, size int64, err error) {
	glog.V(1).Infof("copying dat file of %s to archival %s as %s", fullpath, s.pool, key)
	randomUuid, _ := uuid.NewRandom()
	key = randomUuid.String()
	front, err := pb.NewFrontApi(s.addr)
	if err != nil {
		glog.V(1).Infof("New Front Api failed")
	}

	reqNumber, err := front.MigrateAsync(fullpath, s.pool)
	if err != nil {
		glog.V(1).Infof("MigrateAsync failed")
		return "Failed", 0, err
	}
	for {
		status, err := front.GetAsyncStatus(reqNumber)
		if err != nil {
			glog.V(1).Infof("GetAsyncStatus failed")
			return "failed", 0, err
		}
		fn(status.Migrated, (float32)(status.Migrated))
		if status.Done {
			break
		}
	}
	fileinfo, err := front.GetRawFileInfo(fullpath)
	if err != nil {
		glog.V(1).Infof("GetFileInfo failed")
		return "failed", 0, err
	}
	s.remoteInfo = proto.MarshalTextString(fileinfo)
	return "Success", int64(fileinfo.Size), nil
}

func (s *ArchivalBackendStorage) DownloadFile(fileName, key string, fn func(progressed int64, percentage float32) error) (size int64, err error) {
	front, err := pb.NewFrontApi(s.addr)
	if err != nil {
		glog.V(1).Infof("New Front Api failed")
	}

	reqNumber, err := front.RecallAsync(fileName, true)
	if err != nil {
		glog.V(1).Infof("RecallAsync failed")
		return 0, err
	}
	for {
		status, err := front.GetAsyncStatus(reqNumber)
		if err != nil {
			glog.V(1).Infof("GetAsyncStatus failed")
			return 0, err
		}
		fn(status.Migrated, (float32)(status.Migrated))
		if status.Done {
			break
		}
	}
	fileinfo, err := front.GetFileInfo(fileName)
	if err != nil {
		glog.V(1).Infof("GetFileInfo failed")
		return 0, err
	}
	return int64(fileinfo.Size), nil
}

func (s *ArchivalBackendStorage) DeleteFile(key string) (err error) {
	return
}

func (s *ArchivalBackendStorage) GetRemoteInfo() (remoteInfo string) {
	remoteInfo = s.remoteInfo
	return s.remoteInfo
}

// Implement BackendStorageFile interface
type ArchivalBackendStorageFile struct {
	volDataFile string
	destFile    string
	backend     *ArchivalBackendStorage
	tierInfo    *volume_server_pb.VolumeInfo
}

func (f ArchivalBackendStorageFile) ReadAt(p []byte, off int64) (n int, err error) {
	_, e := f.backend.DownloadFile(f.destFile, "", nil)
	if e != nil {
		return 0, e
	}
	file, ef := os.Open(f.destFile)
	if ef != nil {
		return 0, ef
	}
	n, err = file.ReadAt(p, off)
	glog.V(0).Infof("readat, off: %d, size: %d", off, n)
	return
}

func (f ArchivalBackendStorageFile) WriteAt(p []byte, off int64) (n int, err error) {
	return
}

func (f ArchivalBackendStorageFile) Truncate(off int64) error {
	// return f.destFile.Truncate(off)
	return nil
}

func (f ArchivalBackendStorageFile) Close() error {
	return nil
}

func (f ArchivalBackendStorageFile) GetStat() (datSize int64, modTime time.Time, err error) {

	files := f.tierInfo.GetFiles()

	if len(files) == 0 {
		err = fmt.Errorf("remote file info not found")
		return
	}

	datSize = int64(files[0].FileSize)
	modTime = time.Unix(int64(files[0].ModifiedTime), 0)

	return
}

func (f ArchivalBackendStorageFile) Name() string {
	destFilename := f.volDataFile + ".dat"
	return destFilename
}

func (f ArchivalBackendStorageFile) Sync() error {
	return nil
}
