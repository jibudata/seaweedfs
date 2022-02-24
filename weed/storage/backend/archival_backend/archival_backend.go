package archival_backend

import (
	"fmt"
	"os"
	"time"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/volume_server_pb"
	"github.com/chrislusf/seaweedfs/weed/storage/backend"
	pb "github.com/chrislusf/seaweedfs/weed/storage/backend/archival_backend/archival_api"
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
	pool			string
	managedFsStatus string
}

func newArchivalBackendStorage(conf backend.StringProperties, configPrefix string, id string) (s *ArchivalBackendStorage, err error) {

	f := &ArchivalBackendStorage{
		id:     id,
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

func (s *ArchivalBackendStorage) CopyFile(f *os.File, fn func(progressed int64, percentage float32) error) (key string, size int64, err error) {
	glog.V(1).Infof("copying dat file of %s to archival %s as %s", f.Name(), s.id, key)

	// Original volume file
	info, err := f.Stat()
	if err != nil {
		return key, 0, fmt.Errorf("failed to stat file %q, %v", f.Name(), err)
	}
	fileName := info.Name()
	front, err := pb.NewFrontApi(s.addr)
	if err != nil {
		glog.V(1).Infof("New Front Api failed")
	}

	re, err := front.Migrate(fileName, s.pool)
	if(re) {
		return "Migrated", 100, nil
	} else {
		return "Failed", 0, err
	}
	// New file in fuse mount point
	// destFilename := s.rootFs + "/" + path.Base(f.Name())
	// destFile, err := os.Create(destFilename)
	// glog.V(0).Infof("create dest file: %s", destFilename)
	// if err != nil {
	// 	glog.V(0).Infof("create dest file failed: %s", destFilename)
	// 	return key, 0, fmt.Errorf("failed to create desination file %q, %v", destFilename, err)
	// }
	// defer destFile.Close()

	// glog.V(0).Infof("destination file name in fuse: %s", destFilename)

	// Read from original volume and write to new file
	// var totalWritten int64
	// totalWritten = 0
	// fileSize := info.Size()

	// partSize := int64(64 * 1024 * 1024) // The minimum/default allowed part size is 5MB
	// for partSize*1000 < fileSize {
	// 	partSize *= 4
	// }

	// buffer := make([]byte, partSize)
	// rbuf := bufio.NewReader(f)
	// wbuf := bufio.NewWriter(destFile)

	// for {
	// 	bytesread, err := rbuf.Read(buffer)
	// 	if err != nil && err != io.EOF {
	// 		return key, totalWritten, err
	// 	}
	// 	if bytesread == 0 {
	// 		break
	// 	}

	// 	_, err = wbuf.Write(buffer[:bytesread])
	// 	if err != nil {
	// 		return key, totalWritten, err
	// 	}
	// 	totalWritten += int64(bytesread)

	// 	// Progress function
	// 	if fn != nil {
	// 		if err := fn(totalWritten, float32(totalWritten*100)/float32(fileSize)); err != nil {
	// 			return key, totalWritten, err
	// 		}
	// 	}

	// 	if totalWritten == fileSize {
	// 		break
	// 	}
	// }

	// glog.V(0).Infof("upload complete: %s", destFilename)
	// return key, totalWritten, nil
}

func (s *ArchivalBackendStorage) DownloadFile(fileName, key string, fn func(progressed int64, percentage float32) error) (size int64, err error) {
	front, err := pb.NewFrontApi(s.addr)
	if err != nil {
		glog.V(1).Infof("New Front Api failed")
	}

	re, err := front.Recall(fileName, true)
	if(re) {
		return 100, nil
	} else {
		return  0, err
	}
	// glog.V(0).Infof("copying dat file of from fuse ltfsdm.%s as volume %s", s.id, fileName)

	// // Source file in fuse mount point
	// srcFilename := s.rootFs + "/" + path.Base(fileName)
	// srcFile, err := os.Open(srcFilename)
	// if err != nil {
	// 	return 0, fmt.Errorf("failed to open source file %q, %v", srcFilename, err)
	// }
	// defer srcFile.Close()

	// // Source volume file
	// info, err := srcFile.Stat()
	// if err != nil {
	// 	return 0, fmt.Errorf("failed to stat file %q, %v", srcFilename, err)
	// }

	// // Create destination file for volume
	// destFile, err := os.Create(fileName)
	// if err != nil {
	// 	return 0, fmt.Errorf("failed to open desination file %q, %v", fileName, err)
	// }
	// defer destFile.Close()

	// // Read from original volume and write to new file
	// var totalWritten int64
	// totalWritten = 0
	// fileSize := info.Size()

	// partSize := int64(64 * 1024 * 1024) // The minimum/default allowed part size is 5MB
	// for partSize*1000 < fileSize {
	// 	partSize *= 4
	// }

	// buffer := make([]byte, partSize)
	// rbuf := bufio.NewReader(srcFile)
	// wbuf := bufio.NewWriter(destFile)

	// for {
	// 	if totalWritten == 0 {
	// 		glog.V(0).Infof("first time reading from ltfsdm.%s file %s may take few mins", s.id, fileName)
	// 	}

	// 	bytesread, err := rbuf.Read(buffer)
	// 	if err != nil && err != io.EOF {
	// 		return totalWritten, err
	// 	}
	// 	if bytesread == 0 {
	// 		break
	// 	}

	// 	_, err = wbuf.Write(buffer[:bytesread])
	// 	if err != nil {
	// 		return totalWritten, err
	// 	}
	// 	totalWritten += int64(bytesread)

	// 	// Progress function
	// 	if fn != nil {
	// 		if err := fn(totalWritten, float32(totalWritten*100)/float32(fileSize)); err != nil {
	// 			return totalWritten, err
	// 		}
	// 	}

	// 	if totalWritten == fileSize {
	// 		break
	// 	}
	// }

	// glog.V(0).Infof("download complete: %s", srcFilename)
	// return totalWritten, nil
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
	// n, err = f.destFile.ReadAt(p, off)
	// glog.V(0).Infof("readat, off: %d, size: %d", off, n)
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
	// glog.V(0).Infof("close fuse backend file: %s", f.volDataFile)
	// f.destFile.Close()
	// return nil
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
