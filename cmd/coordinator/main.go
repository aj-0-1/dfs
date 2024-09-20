package main

import (
	"dfs/internal/coordinator"
	pbcoord "dfs/internal/pb/coordinator"
	"log"
	"net"

	"google.golang.org/grpc"
)

func main() {
    metadataAddr := "localhost:50052"
    storageAddrs := []string{"localhost:50051"} // Add more storage node addresses as needed

    server, err := coordinator.NewServer(metadataAddr, storageAddrs)
    if err != nil {
        log.Fatalf("Failed to create coordinator server: %v", err)
    }

    lis, err := net.Listen("tcp", ":50053")
    if err != nil {
        log.Fatalf("Failed to listen: %v", err)
    }

    s := grpc.NewServer()
    pbcoord.RegisterCoordinatorServer(s, server)

    log.Println("Starting Coordinator gRPC server on :50053")
    if err := s.Serve(lis); err != nil {
        log.Fatalf("Failed to serve: %v", err)
    }
}
