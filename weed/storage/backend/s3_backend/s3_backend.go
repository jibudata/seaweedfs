package s3_backend

import (
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/google/uuid"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/volume_server_pb"
	"github.com/chrislusf/seaweedfs/weed/storage/backend"
)

func init() {
	backend.BackendStorageFactories["s3"] = &S3BackendFactory{}
}

type S3BackendFactory struct {
}

func (factory *S3BackendFactory) StorageType() backend.StorageType {
	return backend.StorageType("s3")
}
func (factory *S3BackendFactory) BuildStorage(configuration backend.StringProperties, configPrefix string, id string) (backend.BackendStorage, error) {
	return newS3BackendStorage(configuration, configPrefix, id)
}

type S3BackendStorage struct {
	id                    string
	aws_access_key_id     string
	aws_secret_access_key string
	region                string
	bucket                string
	endpoint              string
	conn                  s3iface.S3API
}

func newS3BackendStorage(configuration backend.StringProperties, configPrefix string, id string) (s *S3BackendStorage, err error) {
	s = &S3BackendStorage{}
	s.id = id
	s.aws_access_key_id = configuration.GetString(configPrefix + "aws_access_key_id")
	s.aws_secret_access_key = configuration.GetString(configPrefix + "aws_secret_access_key")
	s.region = configuration.GetString(configPrefix + "region")
	s.bucket = configuration.GetString(configPrefix + "bucket")
	s.endpoint = configuration.GetString(configPrefix + "endpoint")

	s.conn, err = createSession(s.aws_access_key_id, s.aws_secret_access_key, s.region, s.endpoint)

	glog.V(0).Infof("created backend storage s3.%s for region %s bucket %s", s.id, s.region, s.bucket)
	return
}

func (s *S3BackendStorage) ToProperties() map[string]string {
	m := make(map[string]string)
	m["aws_access_key_id"] = s.aws_access_key_id
	m["aws_secret_access_key"] = s.aws_secret_access_key
	m["region"] = s.region
	m["bucket"] = s.bucket
	m["endpoint"] = s.endpoint
	return m
}

func (s *S3BackendStorage) NewStorageFile(volFileName, key string, tierInfo *volume_server_pb.VolumeInfo) backend.BackendStorageFile {
	if strings.HasPrefix(key, "/") {
		key = key[1:]
	}

	f := &S3BackendStorageFile{
		backendStorage: s,
		key:            key,
		tierInfo:       tierInfo,
	}

	return f
}

func (s *S3BackendStorage) CopyFile(fullpath string, f *os.File, fn func(progressed int64, percentage float32) error) (key string, size int64, err error) {
	randomUuid, _ := uuid.NewRandom()
	key = randomUuid.String()

	glog.V(1).Infof("copying dat file of %s to remote s3.%s as %s", f.Name(), s.id, key)

	size, err = uploadToS3(s.conn, f.Name(), s.bucket, key, fn)

	return
}

func (s *S3BackendStorage) DownloadFile(fileName string, key string, fn func(progressed int64, percentage float32) error) (size int64, err error) {

	glog.V(1).Infof("download dat file of %s from remote s3.%s as %s", fileName, s.id, key)

	size, err = downloadFromS3(s.conn, fileName, s.bucket, key, fn)

	return
}

func (s *S3BackendStorage) DeleteFile(key string) (err error) {

	glog.V(1).Infof("delete dat file %s from remote", key)

	err = deleteFromS3(s.conn, s.bucket, key)

	return
}

func (s *S3BackendStorage) GetRemoteInfo() (remoteInfo string) {
	return ""
}

func (s *S3BackendStorage) SaveRemoteInfoToDataBase(datacenter string, rack string, publicUrl string) (err error) {
	return nil;
}

type S3BackendStorageFile struct {
	backendStorage *S3BackendStorage
	key            string
	tierInfo       *volume_server_pb.VolumeInfo
}

func (s3backendStorageFile S3BackendStorageFile) ReadAt(p []byte, off int64) (n int, err error) {

	bytesRange := fmt.Sprintf("bytes=%d-%d", off, off+int64(len(p))-1)

	// glog.V(0).Infof("read %s %s", s3backendStorageFile.key, bytesRange)

	getObjectOutput, getObjectErr := s3backendStorageFile.backendStorage.conn.GetObject(&s3.GetObjectInput{
		Bucket: &s3backendStorageFile.backendStorage.bucket,
		Key:    &s3backendStorageFile.key,
		Range:  &bytesRange,
	})

	if getObjectErr != nil {
		return 0, fmt.Errorf("bucket %s GetObject %s: %v", s3backendStorageFile.backendStorage.bucket, s3backendStorageFile.key, getObjectErr)
	}
	defer getObjectOutput.Body.Close()

	glog.V(4).Infof("read %s %s", s3backendStorageFile.key, bytesRange)
	glog.V(4).Infof("content range: %s, contentLength: %d", *getObjectOutput.ContentRange, *getObjectOutput.ContentLength)

	for {
		if n, err = getObjectOutput.Body.Read(p); err == nil && n < len(p) {
			p = p[n:]
		} else {
			break
		}
	}

	if err == io.EOF {
		err = nil
	}

	return
}

func (s3backendStorageFile S3BackendStorageFile) WriteAt(p []byte, off int64) (n int, err error) {
	panic("not implemented")
}

func (s3backendStorageFile S3BackendStorageFile) Truncate(off int64) error {
	panic("not implemented")
}

func (s3backendStorageFile S3BackendStorageFile) Close() error {
	return nil
}

func (s3backendStorageFile S3BackendStorageFile) GetStat() (datSize int64, modTime time.Time, err error) {

	files := s3backendStorageFile.tierInfo.GetFiles()

	if len(files) == 0 {
		err = fmt.Errorf("remote file info not found")
		return
	}

	datSize = int64(files[0].FileSize)
	modTime = time.Unix(int64(files[0].ModifiedTime), 0)

	return
}

func (s3backendStorageFile S3BackendStorageFile) Name() string {
	return s3backendStorageFile.key
}

func (s3backendStorageFile S3BackendStorageFile) Sync() error {
	return nil
}
