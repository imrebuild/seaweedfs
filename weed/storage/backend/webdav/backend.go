package webdav

import (
	"fmt"
	"github.com/google/uuid"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/backend"
	"github.com/studio-b12/gowebdav"
	"io"
	"os"
	"strings"
	"time"
)

func init() {
	backend.BackendStorageFactories["webdav"] = &BackendFactory{}
}

type BackendFactory struct {
}

func (factory *BackendFactory) StorageType() backend.StorageType {
	return "webdav"
}

func (factory *BackendFactory) BuildStorage(configuration backend.StringProperties, configPrefix string, id string) (backend.BackendStorage, error) {
	return newBackendStorage(configuration, configPrefix, id)
}

type BackendStorage struct {
	id       string
	username string
	password string
	endpoint string
	conn     *gowebdav.Client
}

func newBackendStorage(configuration backend.StringProperties, configPrefix string, id string) (s *BackendStorage, err error) {
	s = &BackendStorage{}
	s.id = id
	s.username = configuration.GetString(configPrefix + "username")
	s.password = configuration.GetString(configPrefix + "password")
	s.endpoint = configuration.GetString(configPrefix + "endpoint")

	s.conn = gowebdav.NewClient(s.endpoint, s.username, s.password)

	err = s.conn.Connect()

	glog.V(0).Infof("created backend storage webdav.%s on %s", s.id, s.endpoint)
	return
}

func (s *BackendStorage) ToProperties() map[string]string {
	m := make(map[string]string)
	m["username"] = s.username
	m["password"] = s.password
	m["endpoint"] = s.endpoint
	return m
}

func (s *BackendStorage) NewStorageFile(key string, tierInfo *volume_server_pb.VolumeInfo) backend.BackendStorageFile {
	if strings.HasPrefix(key, "/") {
		key = key[1:]
	}

	f := &BackendStorageFile{
		backendStorage: s,
		path:           key,
		tierInfo:       tierInfo,
	}

	return f
}

func (s *BackendStorage) CopyFile(f *os.File, fn func(progressed int64, percentage float32) error) (key string, size int64, err error) {
	randomUuid, _ := uuid.NewRandom()
	key = randomUuid.String()

	glog.V(1).Infof("copying dat file of %s to remote webdav.%s as %s", f.Name(), s.id, key)

	size, err = uploadToWebdav(s.conn, f.Name(), key, fn)

	return
}

func (s *BackendStorage) DownloadFile(fileName string, key string, fn func(progressed int64, percentage float32) error) (size int64, err error) {
	glog.V(1).Infof("download dat file of %s from remote s3.%s as %s", fileName, s.id, key)

	size, err = downloadFromWebdav(s.conn, fileName, key, fn)

	return
}

func (s *BackendStorage) DeleteFile(path string) (err error) {
	glog.V(1).Infof("delete dat file %s from remote", path)

	return deleteFromWebdav(s.conn, path)
}

type BackendStorageFile struct {
	backendStorage *BackendStorage
	path           string
	tierInfo       *volume_server_pb.VolumeInfo
}

func (webdavBackendStorageFile BackendStorageFile) ReadAt(p []byte, off int64) (n int, err error) {
	bytesRange := fmt.Sprintf("bytes=%d-%d", off, off+int64(len(p))-1)

	// glog.V(0).Infof("read %s %s", webdavBackendStorageFile.path, bytesRange)

	streamRange, err := webdavBackendStorageFile.backendStorage.conn.ReadStreamRange(webdavBackendStorageFile.path, off, int64(len(p)))
	if err != nil {
		return 0, fmt.Errorf("GetObject %s: %v", webdavBackendStorageFile.path, err)
	}
	defer streamRange.Close()

	glog.V(4).Infof("read %s %s", webdavBackendStorageFile.path, bytesRange)

	var readCount int
	for {
		p = p[readCount:]
		readCount, err = streamRange.Read(p)
		n += readCount

		if err != nil {
			break
		}
	}

	if err == io.EOF {
		err = nil
	}

	return
}

func (webdavBackendStorageFile BackendStorageFile) WriteAt(p []byte, off int64) (n int, err error) {
	panic("not implemented")
}

func (webdavBackendStorageFile BackendStorageFile) Truncate(off int64) error {
	panic("not implemented")
}

func (webdavBackendStorageFile BackendStorageFile) Close() error {
	return nil
}

func (webdavBackendStorageFile BackendStorageFile) GetStat() (datSize int64, modTime time.Time, err error) {
	files := webdavBackendStorageFile.tierInfo.GetFiles()

	if len(files) == 0 {
		err = fmt.Errorf("remote file info not found")
		return
	}

	datSize = int64(files[0].FileSize)
	modTime = time.Unix(int64(files[0].ModifiedTime), 0)

	return
}

func (webdavBackendStorageFile BackendStorageFile) Name() string {
	return webdavBackendStorageFile.path
}

func (webdavBackendStorageFile BackendStorageFile) Sync() error {
	return nil
}
