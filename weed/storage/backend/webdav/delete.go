package webdav

import (
	"github.com/studio-b12/gowebdav"
)

func deleteFromWebdav(conn *gowebdav.Client, sourcePath string) (err error) {
	return conn.RemoveAll(sourcePath)
}
