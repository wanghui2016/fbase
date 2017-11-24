package server

import (
	"time"
	"io/ioutil"
	"io"
	"github.com/jlaffaye/ftp"
	"util/log"
)

type FileStore struct {
	ftpServer   string
}

type schema struct {
	name string
	version string
	content []byte
	time uint64
}

func NewFileStore(ftpServer string) *FileStore {
	f := &FileStore{
		ftpServer: ftpServer,
	}
	return f
}

func (f *FileStore) Upload(name, version string, content io.Reader) error {
	c, err := ftp.DialTimeout(f.ftpServer, 5*time.Second)
	if err != nil {
		log.Error("ftp connect error: %v", err)
		return err
	}
	defer c.Quit()

	err = c.Login("anonymous", "anonymous")
	if err != nil {
		log.Error("ftp login error: %v", err)
		return err
	}

	err = c.Stor("fbase/"+name+"-"+version+".zip", content)
	if err != nil {
		log.Error("ftp store error: %v", err)
		return err
	}

	// TODO save SQL
	//data, err := ioutil.ReadAll(content)
	//if err != nil {
	//	return err
	//}
	//
	//if err = f.sqlExec(fmt.Sprintf("insert into %s (%s,%s,%s,%s,%s,%s) values (%d,%d,%d,%d,%d,%d)",
	//	TABLE_FILE_STORE,
	//	"name", "version", "content", "time",
	//	name, version, content, time.Now().UnixNano())); err != nil {
	//	return err
	//}
	return nil
}

func (f *FileStore) Download(name, version string) ([]byte, error) {
	c, err := ftp.DialTimeout(f.ftpServer, 5*time.Second)
	if err != nil {
		log.Error("ftp connect error: %v", err)
		return nil, err
	}
	defer c.Quit()

	err = c.Login("anonymous", "anonymous")
	if err != nil {
		log.Error("ftp login error: %v", err)
		return nil, err
	}

	r, err := c.Retr("fbase/"+name+version)
	if err != nil {
		log.Error("ftp read error: %v", err)
		return nil, err
	}
	defer r.Close()

	buf, err := ioutil.ReadAll(r)
	if err != nil {
		log.Error("ftp readall error: %v", err)
		return nil, err
	}
	return buf, nil

	//sch, err := f.sqlQuery("select name, version, content, time from %s where name=%s and version=%s",
	//	TABLE_FILE_STORE, name, version)
	//if err != nil {
	//	log.Error("http file download: sql exec error: %", err)
	//	return
	//}
}
