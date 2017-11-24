package main

import (
	"flag"

	"math"

	"bytes"

	"fmt"

	"engine/rowstore"
)

var dir1 = flag.String("dir1", "", "db1 path")
var dir2 = flag.String("dir2", "", "db2 path")

func main() {
	flag.Parse()
	err := compareDB(*dir1, *dir2)
	fmt.Println("******************************************* diff result start *****************************************************")
	fmt.Println("db1:", *dir1)
	fmt.Println("db2:", *dir2)
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println("consistent!")
	}
	fmt.Println("******************************************* diff result end *****************************************************")
}

func compareDB(path1, path2 string) error {
	db1, _, err := rowstore.OpenFile(path1, nil)
	if err != nil {
		return err
	}
	db2, _, err := rowstore.OpenFile(path2, nil)
	if err != nil {
		return err
	}
	iter1 := db1.NewIterator(nil, nil, math.MaxUint64-1)
	defer iter1.Release()
	iter2 := db2.NewIterator(nil, nil, math.MaxUint64-1)
	defer iter2.Release()
	var index int
	for iter1.Next() && iter2.Next() {
		index++
		key1 := iter1.Key()
		value1 := iter1.Value()
		key2 := iter2.Key()
		value2 := iter2.Value()
		if !bytes.Equal(key1, key2) {
			return fmt.Errorf("mismatch key at %d. \r\nkey1: %v \r\nkey2: %v", index, key1, key2)
		}
		if !bytes.Equal(value1, value2) {
			return fmt.Errorf("mismatch value at %d. \r\nkey: %v. \r\nvalue1:%v\r\nvalue2:%v", index, key1, value1, value2)
		}
	}
	if iter1.Error() != nil {
		return fmt.Errorf("db1 iterator error: %v", iter1.Error())
	}
	if iter2.Error() != nil {
		return fmt.Errorf("db2 iterator error: %v", iter2.Error())
	}
	if iter1.Next() {
		return fmt.Errorf("db1 has more data. next key: %v", iter1.Key())
	}
	if iter2.Next() {
		return fmt.Errorf("db2 has more data. next key: %v", iter2.Key())
	}
	return nil
}
