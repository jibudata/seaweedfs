package archival_backend

import (
	"fmt"
	"os"
	"time"

	"database/sql"

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
	sqlhost         string
	sqluser         string
	sqlpswd         string
	sqlport         string
	sqldb           string
	sqltable        string
}

func newArchivalBackendStorage(conf backend.StringProperties, configPrefix string, id string) (s *ArchivalBackendStorage, err error) {

	f := &ArchivalBackendStorage{
		id:       id,
		addr:     conf.GetString(configPrefix + "addr"),
		pool:     conf.GetString(configPrefix + "pool"),
		sqlhost:  conf.GetString(configPrefix + "sqlhost"),
		sqlport:  conf.GetString(configPrefix + "sqlport"),
		sqluser:  conf.GetString(configPrefix + "sqluser"),
		sqlpswd:  conf.GetString(configPrefix + "sqlpswd"),
		sqldb:    conf.GetString(configPrefix + "sqldb"),
		sqltable: conf.GetString(configPrefix + "sqltable"),
	}
	glog.V(0).Infof("create archival backend storage,id: archival.%s, root path: %s", id, f.addr)
	return f, nil
}

// ArchivalBackendStorage Implement BackendStorage interface
func (s *ArchivalBackendStorage) ToProperties() map[string]string {
	m := make(map[string]string)
	m["addr"] = s.addr
	m["pool"] = s.pool
	m["sqlhost"] = s.sqlhost
	m["sqlport"] = s.sqlport
	m["sqluser"] = s.sqluser
	m["sqlpswd"] = s.sqlpswd
	m["sqldb"] = s.sqldb
	m["sqltable"] = s.sqltable
	return m
}

func (s *ArchivalBackendStorage) NewStorageFile(volFileName, key string, tierInfo *volume_server_pb.VolumeInfo) backend.BackendStorageFile {
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
	glog.V(0).Infof("copying dat file of %s to archival %s as %s", fullpath, s.pool, key)
	randomUuid, _ := uuid.NewRandom()
	key = randomUuid.String()
	front, err := pb.NewFrontApi(s.addr)
	if err != nil {
		glog.V(0).Infof("New Front Api failed")
		return "Failed", 0, err
	}

	reqNumber, err := front.MigrateAsync(fullpath, s.pool)
	if err != nil {
		glog.V(0).Infof("MigrateAsync failed")
		return "Failed", 0, err
	}
	for {
		status, err := front.GetAsyncStatus(reqNumber)
		if err != nil {
			glog.V(0).Infof("GetAsyncStatus failed")
			return "failed", 0, err
		}
		// fn(status.Migrated, (float32)(status.Migrated))
		if status.Done {
			break
		}
	}

	fileinfo, err := front.GetRawFileInfo(fullpath)
	if err != nil {
		glog.V(0).Infof("GetFileInfo failed")
		return "failed", 0, err
	}

	s.remoteInfo = proto.MarshalTextString(fileinfo)
	glog.V(0).Infof("Remote info:%s", s.remoteInfo)

	return key, int64(fileinfo.Size), nil
}

func (s *ArchivalBackendStorage) DownloadFile(fileName, key string, fn func(progressed int64, percentage float32) error) (size int64, err error) {
	front, err := pb.NewFrontApi(s.addr)
	if err != nil {
		glog.V(0).Infof("New Front Api failed")
	}

	reqNumber, err := front.RecallAsync(fileName, true)
	if err != nil {
		glog.V(0).Infof("RecallAsync failed")
		return 0, err
	}
	for {
		status, err := front.GetAsyncStatus(reqNumber)
		if err != nil {
			glog.V(0).Infof("GetAsyncStatus failed")
			return 0, err
		}
		// fn(status.Migrated, (float32)(status.Migrated))
		if status.Done {
			break
		}
	}
	fileinfo, err := front.GetFileInfo(fileName)
	if err != nil {
		glog.V(0).Infof("GetFileInfo failed")
		return 0, err
	}

	s.remoteInfo = ""
	return int64(fileinfo.Size), nil
}

func (s *ArchivalBackendStorage) DeleteFile(key string) (err error) {
	return
}

func (s *ArchivalBackendStorage) GetRemoteInfo() (remoteInfo string) {
	return s.remoteInfo
}

func (s *ArchivalBackendStorage) SaveRemoteInfoToDataBase(datacenter string, rack string, publicUrl string) (err error) {
	sqlUrl := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", s.sqluser, s.sqlpswd, s.sqlhost, s.sqlport, s.sqldb)
	glog.V(0).Infof("open cmd:%s", sqlUrl)
	db, e := sql.Open("mysql", sqlUrl)
	if e != nil {
		glog.V(0).Infof("Open mysql failed with error:%s", e)
		return e;
	}
	db.SetMaxIdleConns(2)
	db.SetMaxOpenConns(100)
	db.SetConnMaxLifetime(time.Duration(0) * time.Second)
	if e = db.Ping(); e != nil {
		glog.V(0).Infof("Ping failed with error:%s", e)
		return e;
	}
	defer db.Close()

	createCmd := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s(ID INT PRIMARY KEY AUTO_INCREMENT, data_center text, rack text, public_url text, volume_id text, volume_info text)", s.sqltable)
	glog.V(0).Infof("create cmd:%s", createCmd)
	_, e = db.Exec(createCmd)
	if e != nil {
		glog.V(0).Infof("Create table failed with error:", e)
	}

	insertCmd := fmt.Sprintf("INSERT INTO %s (data_center, rack, public_url, volume_id, volume_info) VALUES ('%s', '%s', '%s', '%s', '%s')", s.sqltable, datacenter, rack, publicUrl, s.id, s.remoteInfo)
	if s.remoteInfo == "" {
		glog.V(0).Infof("Null remote info, delete the remote info in the data base")
		insertCmd = fmt.Sprintf("DELETE FROM %s WHERE data_center='%s' AND rack='%s' AND public_url='%s' AND volume_id='%s'", s.sqltable, datacenter, rack, publicUrl, s.id)
	}
	glog.V(0).Infof("Insert cmd:%s", insertCmd)
	_, e = db.Exec(insertCmd)
	if e != nil {
		glog.V(0).Infof("Insert failed with error:%s", e)
		return e;
	}
	return nil;
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
