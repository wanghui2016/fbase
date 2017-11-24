package main

import (
	"gateway-server/server"
	"runtime"
	"util/log"
)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU() - 1)

	// load config file
	conf := new(server.Config)
	conf.LoadConfig()
	log.InitFileLog(conf.LogDir,conf.LogModule,conf.LogLevel)
	srv, err := server.NewServer(conf)
	if err != nil {
		log.Fatal("init server failed, err[%v]", err)
		return
	}
	srv.Run()
}
