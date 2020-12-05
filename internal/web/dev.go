// +build dev

package web

import (
	"os"
	"path/filepath"
)

var rootDir string

func init() {
	wd, err := os.Getwd()
	if err != nil {
		panic(err)
	}
	rootDir = filepath.Join(wd, "internal/web/public")
}
