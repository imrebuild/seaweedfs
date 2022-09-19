package webdav

import (
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/studio-b12/gowebdav"
	"os"
	"sync"
)

func uploadToWebdav(conn *gowebdav.Client, filename string, destPath string, fn func(progressed int64, percentage float32) error) (fileSize int64, err error) {
	//open the file
	f, err := os.Open(filename)
	if err != nil {
		return 0, fmt.Errorf("failed to open file %q, %v", filename, err)
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return 0, fmt.Errorf("failed to stat file %q, %v", filename, err)
	}

	fileSize = info.Size()

	fileReader := &UploadProgressedReader{
		fp:      f,
		size:    fileSize,
		signMap: map[int64]struct{}{},
		fn:      fn,
	}

	err = conn.WriteStream(destPath, fileReader, 0644)
	//in case it fails to upload
	if err != nil {
		return 0, fmt.Errorf("failed to upload file %s: %v", filename, err)
	}

	glog.V(1).Infof("file %s uploaded to %s\n", filename, destPath)

	return
}

// UploadProgressedReader adapted from https://github.com/aws/aws-sdk-go/pull/1868
// https://github.com/aws/aws-sdk-go/blob/main/example/service/s3/putObjectWithProcess/putObjWithProcess.go
type UploadProgressedReader struct {
	fp      *os.File
	size    int64
	read    int64
	signMap map[int64]struct{}
	mux     sync.Mutex
	fn      func(progressed int64, percentage float32) error
}

func (r *UploadProgressedReader) Read(p []byte) (int, error) {
	return r.fp.Read(p)
}

func (r *UploadProgressedReader) ReadAt(p []byte, off int64) (int, error) {
	n, err := r.fp.ReadAt(p, off)
	if err != nil {
		return n, err
	}

	r.mux.Lock()
	// Ignore the first signature call
	if _, ok := r.signMap[off]; ok {
		r.read += int64(n)
	} else {
		r.signMap[off] = struct{}{}
	}
	r.mux.Unlock()

	if r.fn != nil {
		read := r.read
		if err := r.fn(read, float32(read*100)/float32(r.size)); err != nil {
			return n, err
		}
	}

	return n, err
}

func (r *UploadProgressedReader) Seek(offset int64, whence int) (int64, error) {
	return r.fp.Seek(offset, whence)
}
