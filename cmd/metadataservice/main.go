package main

import (
	"dfs/internal/metadataservice"
	pb "dfs/internal/pb/metadata"
	"log"
	"net"

	"google.golang.org/grpc"
)

func main() {
	store, err := metadataservice.NewDiskStore("/tmp/dfs-metadata")
	if err != nil {
		log.Fatalf("Failed to create metadata store: %v", err)
	}

	server := metadataservice.NewServer(store)

	lis, err := net.Listen("tcp", ":50052")
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	s := grpc.NewServer()
	pb.RegisterMetadataServiceServer(s, server)

	log.Println("Starting Metadata gRPC server on :50052")
	if err := s.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}
