package main

import (
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"data-server/server"
	"raft/logger"
	"util/log"
)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU() - 1)

	// load config file
	conf := new(server.Config)
	conf.LoadConfig()
	log.InitFileLog(conf.LogDir, conf.LogModule, conf.LogLevel)
	// config raft logger
	logger.SetLogger(log.GetFileLogger())
	s := server.InitServer(conf)

	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, syscall.SIGHUP, syscall.SIGINT, syscall.SIGQUIT, syscall.SIGTERM)
	go func() {
		sig := <-signalCh
		log.Warn("signal[%v] caught. server exit...", sig)
		s.Stop()
		time.Sleep(time.Second)
		os.Exit(0)
	}()

	s.Start()
}
