package main

import (
	"flag"
	"log"
	"strings"

	"github.com/bajh/raftwerk/raftlog"
	"github.com/bajh/raftwerk/server"
	"github.com/bajh/raftwerk/store"
)

func main() {
	port := flag.String("port", ":9447", "port to listen on")
	logFile := flag.String("log", "./raftt_log", "location of log file")
	id := flag.String("id", "localhost:9447", "id of server")
	peers := flag.String("peers", "localhost:9448,localhost:9449,localhost:9450", "comma separated list of peers")
	flag.Parse()

	l := raftlog.NewFileLog(*logFile)

	cluster := server.NewCluster(l)
	s := server.NewServer(*id, store.NewMemStore(), l, cluster)
	for _, addr := range strings.Split(*peers, ",") {
		cli, err := server.NewPeer(addr, addr)
		if err != nil {
			log.Fatalf("creating client: %v", err)
		}
		cluster.AddPeer(cli)
	}

	log.Println("listening on", *port)
	if err := s.ListenAndServe(*port); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
