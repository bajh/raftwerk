package main

import (
	"context"
	"flag"
	"fmt"
	"log"

	pb "github.com/bajh/raftwerk/transport"

	"google.golang.org/grpc"
)

func main() {
	port := flag.String("port", ":9447", "port to listen on")
	flag.Parse()
	addr := fmt.Sprintf("localhost%s", *port)

	conn, err := grpc.DialContext(context.Background(), addr, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Fatalf("could not connect: %v", err)
	}
	defer conn.Close()

	args := flag.Args()
	cmd := args[0]
	switch cmd {
	case "add-peer":
		cli := pb.NewRaftClient(conn)
		_, err = cli.AddPeer(context.Background(), &pb.AddPeerRequest{
			ID:   args[2],
			Host: args[2],
		})
		if err != nil {
			log.Fatal("could not get val:", err)
		}
	case "set":
		cli := pb.NewKeyValStoreClient(conn)
		_, err = cli.Set(context.Background(), &pb.SetRequest{
			Key: args[1],
			Val: args[2],
		})
	default:
		log.Fatal("invalid command: command must be one of (get|set|del)")
	}

}
