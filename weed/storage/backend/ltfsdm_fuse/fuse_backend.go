package ltfsdm_fuse

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"path"
	"time"

	"github.com/google/uuid"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/volume_server_pb"
	"github.com/chrislusf/seaweedfs/weed/storage/backend"
)

func init() {
	backend.BackendStorageFactories["ltfsdm"] = &FuseBackendFactory{}
}

// LTFSDM status
const (
	LTFSDMStateRunning    = "running"
	LTFSDMStateNotManaged = "unmanaged"
	LTFSDMStateDown       = "down"
)

type FuseBackendFactory struct {
}

func (f *FuseBackendFactory) StorageType() backend.StorageType {
	return backend.StorageType("ltfsdm")
}

func (f *FuseBackendFactory) BuildStorage(conf backend.StringProperties, configPrefix string, id string) (backend.BackendStorage, error) {
	return newFuseBackendStorage(conf, configPrefix, id)
}

type FuseBackendStorage struct {
	id              string
	rootFs          string
	managedFsStatus string
}

func newFuseBackendStorage(conf backend.StringProperties, configPrefix string, id string) (s *FuseBackendStorage, err error) {

	f := &FuseBackendStorage{
		id:     id,
		rootFs: conf.GetString(configPrefix + "root_path"),
	}
	glog.V(0).Infof("create ltfsdm fuse backend storage,id: ltfsdm.%s, root path: %s", id, f.rootFs)

	return f, nil
}

// FuseBackendStorage Implement BackendStorage interface
func (s *FuseBackendStorage) ToProperties() map[string]string {
	m := make(map[string]string)
	m["root_path"] = s.rootFs
	return m
}

func (s *FuseBackendStorage) NewStorageFile(volFileName, key string, tierInfo *volume_server_pb.VolumeInfo) backend.BackendStorageFile {
	// Open file from fuse mount point
	destFilename := s.rootFs + "/" + path.Base(volFileName) + ".dat"
	destFile, err := os.Open(destFilename)

	if err != nil {
		return nil
	}

	return &FuseBackendStorageFile{
		volDataFile: volFileName,
		destFile:    destFile,
		backend:     s,
		tierInfo:    tierInfo,
	}
}

func (s *FuseBackendStorage) CopyFile(fullpath string, f *os.File, fn func(progressed int64, percentage float32) error) (key string, size int64, err error) {
	randomUuid, _ := uuid.NewRandom()
	key = randomUuid.String()

	glog.V(1).Infof("copying dat file of %s to fuse ltfsdm.%s as %s", f.Name(), s.id, key)

	// Original volume file
	info, err := f.Stat()
	if err != nil {
		return key, 0, fmt.Errorf("failed to stat file %q, %v", f.Name(), err)
	}

	// New file in fuse mount point
	destFilename := s.rootFs + "/" + path.Base(f.Name())
	destFile, err := os.Create(destFilename)
	glog.V(0).Infof("create dest file: %s", destFilename)
	if err != nil {
		glog.V(0).Infof("create dest file failed: %s", destFilename)
		return key, 0, fmt.Errorf("failed to create desination file %q, %v", destFilename, err)
	}
	defer destFile.Close()

	glog.V(0).Infof("destination file name in fuse: %s", destFilename)

	// Read from original volume and write to new file
	var totalWritten int64
	totalWritten = 0
	fileSize := info.Size()

	partSize := int64(64 * 1024 * 1024) // The minimum/default allowed part size is 5MB
	for partSize*1000 < fileSize {
		partSize *= 4
	}

	buffer := make([]byte, partSize)
	rbuf := bufio.NewReader(f)
	wbuf := bufio.NewWriter(destFile)

	for {
		bytesread, err := rbuf.Read(buffer)
		if err != nil && err != io.EOF {
			return key, totalWritten, err
		}
		if bytesread == 0 {
			break
		}

		_, err = wbuf.Write(buffer[:bytesread])
		if err != nil {
			return key, totalWritten, err
		}
		totalWritten += int64(bytesread)

		// Progress function
		if fn != nil {
			if err := fn(totalWritten, float32(totalWritten*100)/float32(fileSize)); err != nil {
				return key, totalWritten, err
			}
		}

		if totalWritten == fileSize {
			break
		}
	}

	glog.V(0).Infof("upload complete: %s", destFilename)
	return key, totalWritten, nil
}

func (s *FuseBackendStorage) DownloadFile(fileName, key string, fn func(progressed int64, percentage float32) error) (size int64, err error) {
	glog.V(0).Infof("copying dat file of from fuse ltfsdm.%s as volume %s", s.id, fileName)

	// Source file in fuse mount point
	srcFilename := s.rootFs + "/" + path.Base(fileName)
	srcFile, err := os.Open(srcFilename)
	if err != nil {
		return 0, fmt.Errorf("failed to open source file %q, %v", srcFilename, err)
	}
	defer srcFile.Close()

	// Source volume file
	info, err := srcFile.Stat()
	if err != nil {
		return 0, fmt.Errorf("failed to stat file %q, %v", srcFilename, err)
	}

	// Create destination file for volume
	destFile, err := os.Create(fileName)
	if err != nil {
		return 0, fmt.Errorf("failed to open desination file %q, %v", fileName, err)
	}
	defer destFile.Close()

	// Read from original volume and write to new file
	var totalWritten int64
	totalWritten = 0
	fileSize := info.Size()

	partSize := int64(64 * 1024 * 1024) // The minimum/default allowed part size is 5MB
	for partSize*1000 < fileSize {
		partSize *= 4
	}

	buffer := make([]byte, partSize)
	rbuf := bufio.NewReader(srcFile)
	wbuf := bufio.NewWriter(destFile)

	for {
		if totalWritten == 0 {
			glog.V(0).Infof("first time reading from ltfsdm.%s file %s may take few mins", s.id, fileName)
		}

		bytesread, err := rbuf.Read(buffer)
		if err != nil && err != io.EOF {
			return totalWritten, err
		}
		if bytesread == 0 {
			break
		}

		_, err = wbuf.Write(buffer[:bytesread])
		if err != nil {
			return totalWritten, err
		}
		totalWritten += int64(bytesread)

		// Progress function
		if fn != nil {
			if err := fn(totalWritten, float32(totalWritten*100)/float32(fileSize)); err != nil {
				return totalWritten, err
			}
		}

		if totalWritten == fileSize {
			break
		}
	}

	glog.V(0).Infof("download complete: %s", srcFilename)
	return totalWritten, nil
}

func (s *FuseBackendStorage) DeleteFile(key string) (err error) {
	return
}

func (s *FuseBackendStorage) GetRemoteInfo() (remoteInfo string) {
	return ""
}

// Implement BackendStorageFile interface
type FuseBackendStorageFile struct {
	volDataFile string
	destFile    *os.File
	backend     *FuseBackendStorage
	tierInfo    *volume_server_pb.VolumeInfo
}

func (f FuseBackendStorageFile) ReadAt(p []byte, off int64) (n int, err error) {

	n, err = f.destFile.ReadAt(p, off)
	glog.V(0).Infof("readat, off: %d, size: %d", off, n)
	return
}

func (f FuseBackendStorageFile) WriteAt(p []byte, off int64) (n int, err error) {
	panic("not implemented")
}

func (f FuseBackendStorageFile) Truncate(off int64) error {
	return f.destFile.Truncate(off)
}

func (f FuseBackendStorageFile) Close() error {
	glog.V(0).Infof("close fuse backend file: %s", f.volDataFile)
	f.destFile.Close()
	return nil
}

func (f FuseBackendStorageFile) GetStat() (datSize int64, modTime time.Time, err error) {

	files := f.tierInfo.GetFiles()

	if len(files) == 0 {
		err = fmt.Errorf("remote file info not found")
		return
	}

	datSize = int64(files[0].FileSize)
	modTime = time.Unix(int64(files[0].ModifiedTime), 0)

	return
}

func (f FuseBackendStorageFile) Name() string {
	destFilename := f.backend.rootFs + "/" + path.Base(f.volDataFile) + ".dat"
	return destFilename
}

func (f FuseBackendStorageFile) Sync() error {
	return nil
}
