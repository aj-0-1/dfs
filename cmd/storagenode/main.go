package main

import (
	"dfs/internal/chunk"
	pb "dfs/internal/pb/storagenode"
	"dfs/internal/storagenode"
	"flag"
	"fmt"
	"log"
	"net"
	"os"

	"google.golang.org/grpc"
)

func main() {
    port := flag.Int("port", 50051, "The server port")
    flag.Parse()

    log.Printf("Starting Storage Node on port %d", *port)

    baseDir := os.Getenv("DFS_STORAGE_DIR")
    if baseDir == "" {
        baseDir = fmt.Sprintf("/tmp/dfs-storage-%d", *port)
    }

    store, err := chunk.NewDiskStore(baseDir)
    if err != nil {
        log.Fatalf("Failed to create store: %v", err)
    }

    server := storagenode.NewServer(store)

    lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
    if err != nil {
        log.Fatalf("Failed to listen: %v", err)
    }

    s := grpc.NewServer()
    pb.RegisterStorageNodeServer(s, server)

    log.Printf("Storage Node is listening on :%d", *port)
    if err := s.Serve(lis); err != nil {
        log.Fatalf("Failed to serve: %v", err)
    }
}
