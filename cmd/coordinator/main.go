package main

import (
	"dfs/internal/coordinator"
	pbcoord "dfs/internal/pb/coordinator"
	"log"
	"net"
	"os"
	"strings"

	"google.golang.org/grpc"
)

func main() {
    log.Println("Starting Coordinator")

    metadataAddr := os.Getenv("DFS_METADATA_ADDR")
    if metadataAddr == "" {
        metadataAddr = "metadataservice:50052"
    }

    storageAddrsStr := os.Getenv("DFS_STORAGE_ADDRS")
    if storageAddrsStr == "" {
        storageAddrsStr = "storagenode1:50051,storagenode2:50061,storagenode3:50071"
    }
    storageAddrs := strings.Split(storageAddrsStr, ",")

    server, err := coordinator.NewServer(metadataAddr, storageAddrs)
    if err != nil {
        log.Fatalf("Failed to create coordinator server: %v", err)
    }

    port := os.Getenv("DFS_COORDINATOR_PORT")
    if port == "" {
        port = "50053"
    }

    lis, err := net.Listen("tcp", ":"+port)
    if err != nil {
        log.Fatalf("Failed to listen: %v", err)
    }

    s := grpc.NewServer()
    pbcoord.RegisterCoordinatorServer(s, server)

    log.Printf("Coordinator is listening on :%s", port)
    if err := s.Serve(lis); err != nil {
        log.Fatalf("Failed to serve: %v", err)
    }
}
