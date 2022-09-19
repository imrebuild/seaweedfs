package webdav

import (
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/studio-b12/gowebdav"
	"io"
	"os"
	"sync/atomic"
)

func downloadFromWebdav(conn *gowebdav.Client, destFileName string, sourcePath string,
	fn func(progressed int64, percentage float32) error) (fileSize int64, err error) {

	fileSize, err = getFileSize(conn, sourcePath)
	if err != nil {
		return
	}

	//open the file
	f, err := os.OpenFile(destFileName, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return 0, fmt.Errorf("failed to open file %q, %v", destFileName, err)
	}
	defer f.Close()

	fileWriter := &DownloadProgressedWriter{
		fp:      f,
		size:    fileSize,
		written: 0,
		fn:      fn,
	}

	stream, err := conn.ReadStream(sourcePath)
	defer stream.Close()
	buf := make([]byte, 8)
	_, err = io.CopyBuffer(fileWriter, stream, buf)
	if err != nil {
		return fileSize, fmt.Errorf("failed to download /%s to %s: %v", sourcePath, destFileName, err)
	}

	glog.V(1).Infof("downloaded file %s\n", destFileName)

	return
}

// DownloadProgressedWriter adapted from https://github.com/aws/aws-sdk-go/pull/1868
// and https://petersouter.xyz/s3-download-progress-bar-in-golang/
type DownloadProgressedWriter struct {
	size    int64
	written int64
	fn      func(progressed int64, percentage float32) error
	fp      *os.File
}

func (w *DownloadProgressedWriter) WriteAt(p []byte, off int64) (int, error) {
	n, err := w.fp.WriteAt(p, off)
	if err != nil {
		return n, err
	}

	// Got the length have read( or means has uploaded), and you can construct your message
	atomic.AddInt64(&w.written, int64(n))

	if w.fn != nil {
		written := w.written
		if err := w.fn(written, float32(written*100)/float32(w.size)); err != nil {
			return n, err
		}
	}

	return n, err
}

func (w *DownloadProgressedWriter) Write(p []byte) (int, error) {
	n, err := w.fp.Write(p)
	if err != nil {
		return n, err
	}

	// Got the length have read( or means has uploaded), and you can construct your message
	atomic.AddInt64(&w.written, int64(n))

	if w.fn != nil {
		written := w.written
		if err := w.fn(written, float32(written*100)/float32(w.size)); err != nil {
			return n, err
		}
	}

	return n, err
}

func getFileSize(conn *gowebdav.Client, path string) (int64, error) {
	info, err := conn.Stat(path)

	if err != nil {
		return 0, err
	}

	return info.Size(), nil
}
