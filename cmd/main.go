//@fdb

package main

import (
	"flag"
	"log"
	"net/http"
	_ "net/http/pprof"
	"runtime"

	"mgr"
	"node"
	"util"
)

const (
	Version = "0.1"
)

var (
	configFile = flag.String("c", "", "config file path")
)

func main() {
	log.Println("*Flat Data Store*")

	flag.Parse()
	cfg, e := util.NewConfig(*configFile)
	if e != nil {
		log.Println("Bad config file, ", e)
		return
	}
	role := cfg.GetString("role")

	//for multi-cpu scheduling
	numCPU := runtime.NumCPU()
	runtime.GOMAXPROCS(numCPU)

	profPort := cfg.GetString("prof")
	if len(profPort) != 0 {
		go func() {
			log.Println(http.ListenAndServe(":"+profPort, nil))
		}()
	}

	switch role {
	case "mgr":
		server, err := mgr.NewServer(cfg)
		if err == nil {
			server.Start()
		}

	case "node":
		server, err := node.NewServer(cfg)
		if err == nil {
			server.Start()
		}

	default:
		log.Println("Invalid Server Role")
	}

	return
}
