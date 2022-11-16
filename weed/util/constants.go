package util

import (
	"fmt"
)

var (
	VERSION_NUMBER = fmt.Sprintf("%.02f", 3.35)
	VERSION        = sizeLimit + " " + VERSION_NUMBER
	COMMIT         = ""
)

func Version() string {
	return VERSION + " " + COMMIT
}
