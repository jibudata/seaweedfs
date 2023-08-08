package archival_backend

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/google/uuid"
	"github.com/jibudata/archive-api/client"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/backend"
	"k8s.io/apimachinery/pkg/util/wait"
)

const (
	RecallTimeout = 30 * time.Minute
	RecallInerval = 10 * time.Second
)

func init() {
	backend.BackendStorageFactories["archival"] = &ArchivalBackendFactory{}
}

type ArchivalBackendFactory struct {
}

func (f *ArchivalBackendFactory) StorageType() backend.StorageType {
	return backend.StorageType("archival")
}

func (f *ArchivalBackendFactory) BuildStorage(conf backend.StringProperties, configPrefix string, id string) (backend.BackendStorage, error) {
	return newArchivalBackendStorage(conf, configPrefix, id)
}

type ArchivalBackendStorage struct {
	id       string
	endpoint string
	pool     string
}

func newArchivalBackendStorage(conf backend.StringProperties, configPrefix string, id string) (s *ArchivalBackendStorage, err error) {

	f := &ArchivalBackendStorage{
		id:       id,
		endpoint: conf.GetString(configPrefix + "endpoint"),
		pool:     conf.GetString(configPrefix + "pool"),
	}
	glog.V(0).Infof("create archival backend storage,id: archival.%s, root path: %s", id, f.endpoint)

	return f, nil
}

// ArchivalBackendStorage Implement BackendStorage interface
func (s *ArchivalBackendStorage) ToProperties() map[string]string {
	m := make(map[string]string)
	m["endpoint"] = s.endpoint
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

func (s *ArchivalBackendStorage) CopyFile(f *os.File, fn func(progressed int64, percentage float32) error) (key string, size int64, err error) {
	randomUuid, _ := uuid.NewRandom()
	key = randomUuid.String()

	glog.V(1).Infof("copying dat file %s to archival %s as %s", f.Name(), s.pool, key)

	archiveClient, err := client.NewArchivalClient(s.endpoint)
	if err != nil {
		glog.V(1).Infof("New ArchiveClient failed")
	}

	fullPath, err := filepath.Abs(f.Name())
	if err != nil {
		glog.Errorf("get full path error for volume %s error: %v", f.Name(), err)
		return key, 0, err
	}
	reqNumber, err := archiveClient.MigrateAsync(fullPath, s.pool)
	if err != nil {
		glog.V(1).Infof("MigrateAsync failed")
		return key, 0, err
	}

	ctx, timeoutFunc := context.WithTimeout(context.TODO(), RecallTimeout)
	defer timeoutFunc()

	var err1 error
	wait.UntilWithContext(ctx, func(_ context.Context) {
		status, err := archiveClient.GetAsyncStatus(reqNumber)
		if err != nil {
			glog.V(1).Infof("GetAsyncStatus failed")
			err1 = fmt.Errorf("GetAsyncStatus failed for migration: %v", err)
			return
		}
		fn(status.Migrated, (float32)(status.Migrated))
		if status.Done {
			return
		}

	}, RecallInerval)

	if err1 != nil {
		return key, 0, err1
	}

	fileinfo, err := archiveClient.GetFileInfo(fullPath)
	if err != nil {
		glog.V(1).Infof("GetFileInfo failed")
		return key, 0, err
	}

	return key, int64(fileinfo.Size), nil
}

func (s *ArchivalBackendStorage) DownloadFile(fileName, key string, fn func(progressed int64, percentage float32) error) (size int64, err error) {
	archiveClient, err := client.NewArchivalClient(s.endpoint)
	if err != nil {
		glog.V(1).Infof("New ArchiveClient failed")
	}

	reqNumber, err := archiveClient.RecallAsync(fileName, true)
	if err != nil {
		glog.V(1).Infof("RecallAsync failed")
		return 0, err
	}
	for {
		status, err := archiveClient.GetAsyncStatus(reqNumber)
		if err != nil {
			glog.V(1).Infof("GetAsyncStatus failed")
			return 0, err
		}
		fn(status.Migrated, (float32)(status.Migrated))
		if status.Done {
			break
		}
	}
	fileinfo, err := archiveClient.GetFileInfo(fileName)
	if err != nil {
		glog.V(1).Infof("GetFileInfo failed")
		return 0, err
	}
	return int64(fileinfo.Size), nil
}

func (s *ArchivalBackendStorage) DeleteFile(key string) (err error) {
	return
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
