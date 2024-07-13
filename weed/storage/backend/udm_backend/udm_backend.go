package udmbackend

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"syscall"
	"time"

	"github.com/pkg/xattr"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/backend"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

const (
	BackendIOTimeoutValue = time.Second * 30
	ExtendedAttributeName = "trusted.udm.recall"
)

type RecallState string

const (
	RecallStateCompleted RecallState = "completed"
	RecallStateFailed    RecallState = "failed"
)

type RecallInfo struct {
	TimeStamp string
	State     string
}

func init() {
	backend.BackendStorageFactories["udm"] = &UDMBackendFactory{}
}

type UDMBackendFactory struct {
}

func (factory *UDMBackendFactory) StorageType() backend.StorageType {
	return backend.StorageType("udm")
}

func (factory *UDMBackendFactory) BuildStorage(configuration backend.StringProperties, configPrefix string, id string) (backend.BackendStorage, error) {
	return newUDMBackendStorage(configuration, configPrefix, id)
}

type UDMBackendStorage struct {
	id         string
	mediumType string
}

func newUDMBackendStorage(configuration backend.StringProperties, configPrefix string, id string) (*UDMBackendStorage, error) {
	return &UDMBackendStorage{
		id:         id,
		mediumType: configuration.GetString(configPrefix + "storage_medium"),
	}, nil
}

func (u *UDMBackendStorage) ToProperties() map[string]string {
	m := make(map[string]string)
	m["name"] = u.id
	return m
}

func (u *UDMBackendStorage) CopyFile(f *os.File, fn func(progressed int64, percentage float32) error) (key string, size int64, err error) {
	glog.V(1).Infof("CopyFile %s is called", f.Name())

	key = f.Name()
	// get file size for f
	fileInfo, err := f.Stat()
	size = fileInfo.Size()

	return
}

func (u *UDMBackendStorage) DownloadFile(fileName string, key string, fn func(progressed int64, percentage float32) error) (size int64, err error) {
	glog.V(1).Infof("DownloadFile %s is called", fileName)

	if _, err := os.Stat(fileName); os.IsNotExist(err) {
		glog.V(1).Infof("file %s not exists", fileName)
		return -1, err
	}

	fileInfo, err := os.Stat(fileName)
	size = fileInfo.Size()

	return
}

func (u *UDMBackendStorage) DeleteFile(key string) (err error) {
	return
}

func (u *UDMBackendStorage) NewStorageFile(key string, tierInfo *volume_server_pb.VolumeInfo) backend.BackendStorageFile {
	return &UDMBackendStorageFile{
		backendStorage: u,
		originalName:   key,
		tierInfo:       tierInfo,
	}
}

type UDMBackendStorageFile struct {
	backendStorage *UDMBackendStorage
	originalName   string
	tierInfo       *volume_server_pb.VolumeInfo
}

func (udmStorageFile UDMBackendStorageFile) ReadAt(p []byte, off int64) (n int, err error) {
	// 1. create originalName file
	// creation of this file will be detected by udm, and trigger file recall,
	// so in theory, after a few minutes, the file should be ready for read.
	file, err := os.Create(udmStorageFile.originalName)
	if err != nil {
		glog.V(1).Infof("create file %s failed, err: %v", udmStorageFile.originalName, err)
		return 0, err
	}

	err = SetRecallInfo(file.Name())
	if err != nil {
		glog.V(1).Infof("set recall info failed for file: %s", file.Name())
		return 0, err
	}

	var success bool
	ctx, cancel := context.WithTimeout(context.Background(), BackendIOTimeoutValue)
	defer cancel()

	// 2. wait for file ready to read
	util.RetryUntil("waitForFileRecallCompletion", func() error {
		success, err = waitForFileRecallCompletion(file)
		return err
	}, func(err error) bool {
		glog.Errorf("process %v: %v", file.Name(), err)
		if ctx.Err() == context.DeadlineExceeded {
			err = syscall.EAGAIN
			return false
		}
		if success {
			err = nil
			return false
		}
		return true
	})

	return
}

func (udmStorageFile UDMBackendStorageFile) WriteAt(p []byte, off int64) (n int, err error) {
	panic("not implemented")
}

func (udmStorageFile UDMBackendStorageFile) Truncate(off int64) error {
	panic("not implemented")
}

func (udmStorageFile UDMBackendStorageFile) Close() error {
	return nil
}

func (udmStorageFile UDMBackendStorageFile) GetStat() (datSize int64, modTime time.Time, err error) {

	// check originalName file exists
	// if not exists, return error
	if _, err := os.Stat(udmStorageFile.originalName); os.IsNotExist(err) {
		glog.V(1).Infof("file %s not exists", udmStorageFile.originalName)
		return -1, time.Now(), err
	}

	// just open originalName and get file size and mod time
	fileInfo, err := os.Stat(udmStorageFile.originalName)

	datSize = fileInfo.Size()
	modTime = fileInfo.ModTime()

	return
}

func (udmStorageFile UDMBackendStorageFile) Name() string {
	return udmStorageFile.originalName
}

func (udmStorageFile UDMBackendStorageFile) Sync() error {
	return nil
}

func SetRecallInfo(filename string) error {
	info := &RecallInfo{
		TimeStamp: time.Now().Format(time.RFC3339),
	}
	data, err := json.Marshal(info)
	if err != nil {
		return err
	}
	return xattr.Set(filename, ExtendedAttributeName, data)
}

func GetRecallInfo(filename string) (*RecallInfo, error) {
	data, err := xattr.Get(filename, ExtendedAttributeName)
	if err != nil {
		return nil, err
	}
	var info RecallInfo
	if err := json.Unmarshal(data, &info); err != nil {
		return nil, err
	}
	return &info, nil
}

func waitForFileRecallCompletion(file *os.File) (bool, error) {
	recallInfo, err := GetRecallInfo(file.Name())
	if err != nil {
		glog.V(1).Infof("failed to get recall info for %s", file.Name())
		return false, err
	}

	if recallInfo.State == string(RecallStateCompleted) {
		return true, nil
	} else if recallInfo.State == string(RecallStateFailed) {
		glog.V(1).Infof("failed to recall %s", file.Name())
		return false, errors.New("recall failed")
	}
	return false, nil
}
