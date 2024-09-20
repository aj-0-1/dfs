package main

import (
	"dfs/internal/chunk"
	pb "dfs/internal/pb/storagenode"
	"dfs/internal/storagenode"
	"log"
	"net"

	"google.golang.org/grpc"
)

func main() {
    store, err := chunk.NewDiskStore("/tmp/dfs-storage")
    if err != nil {
        log.Fatalf("Failed to create store: %v", err)
    }

    server := storagenode.NewServer(store)

    lis, err := net.Listen("tcp", ":50051")
    if err != nil {
        log.Fatalf("Failed to listen: %v", err)
    }

    s := grpc.NewServer()
    pb.RegisterStorageNodeServer(s, server)

    log.Println("Starting gRPC server on :50051")
    if err := s.Serve(lis); err != nil {
        log.Fatalf("Failed to serve: %v", err)
    }
}
