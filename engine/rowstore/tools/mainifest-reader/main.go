package main

import (
	"engine/rowstore"
	"engine/rowstore/filesystem"

	"flag"
	"fmt"
)

var path = flag.String("path", ".", "db path")

func main() {
	flag.Parse()

	// open path
	fs, err := filesystem.OpenFile(*path, true)
	if err != nil {
		panic(err)
	}
	defer fs.Close()

	// get mainfest filename
	fd, err := fs.GetMeta()
	if err != nil {
		panic(err)
	}

	// new mainifest reader
	var reader filesystem.Reader
	reader, err = fs.Open(fd)
	if err != nil {
		panic(err)
	}
	defer reader.Close()

	var rec rowstore.SessionRecord
	err = rec.Decode(reader)
	if err != nil {
		panic(err)
	}

	fmt.Println(rec.String())
}
