package server

import (
	"os"
	"util/log"
)

func dirUsage(currPath string) uint64 {
	defer func() {
		if x := recover(); x != nil {
			log.Error("diskusage recover: %v", x)
		}
	}()
	var size uint64

	dir, err := os.Open(currPath)
	if err != nil {
		return 0
	}
	defer dir.Close()

	files, err := dir.Readdir(-1)
	if err != nil {
		return 0
	}

	for _, file := range files {
		if file.IsDir() {
			size += dirUsage(currPath+"/"+file.Name())
		} else {
			size += uint64(file.Size())
		}
	}

	return size
}