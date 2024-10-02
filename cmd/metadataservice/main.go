package main

import (
	"dfs/internal/metadataservice"
	pb "dfs/internal/pb/metadata"
	"log"
	"net"
	"os"

	"google.golang.org/grpc"
)

func main() {
	log.Println("Starting Metadata Service")

	baseDir := os.Getenv("DFS_METADATA_DIR")
	if baseDir == "" {
		baseDir = "/tmp/dfs-metadata"
	}

	store, err := metadataservice.NewDiskStore(baseDir)
	if err != nil {
		log.Fatalf("Failed to create metadata store: %v", err)
	}

	server := metadataservice.NewServer(store)

	port := os.Getenv("DFS_METADATA_PORT")
	if port == "" {
		port = "50052"
	}

	lis, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	s := grpc.NewServer()
	pb.RegisterMetadataServiceServer(s, server)

	log.Printf("Metadata Service is listening on :%s", port)
	if err := s.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}
