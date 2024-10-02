package metadataservice

import (
	"context"
	pb "dfs/internal/pb/metadata"
	"log"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Server struct {
	pb.UnimplementedMetadataServiceServer
	store Store
}

func NewServer(store Store) *Server {
	return &Server{store: store}
}

func (s *Server) SaveFileMetadata(ctx context.Context, req *pb.SaveFileMetadataRequest) (*pb.SaveFileMetadataResponse, error) {
	log.Printf("Saving metadata for file: %s", req.Metadata.FileId)
	meta := &FileMetadata{
		FileID:    req.Metadata.FileId,
		FileName:  req.Metadata.FileName,
		FileSize:  req.Metadata.FileSize,
		ChunkIDs:  req.Metadata.ChunkIds,
		CreatedAt: time.Now().UTC().Format(time.RFC3339),
		UpdatedAt: time.Now().UTC().Format(time.RFC3339),
	}

	if err := s.store.Save(meta); err != nil {
		log.Printf("Failed to save metadata for file %s: %v", req.Metadata.FileId, err)
		return nil, status.Errorf(codes.Internal, "failed to save metadata: %v", err)
	}

	log.Printf("Metadata saved successfully for file: %s", req.Metadata.FileId)
	return &pb.SaveFileMetadataResponse{Success: true}, nil
}

func (s *Server) GetFileMetadata(ctx context.Context, req *pb.GetFileMetadataRequest) (*pb.GetFileMetadataResponse, error) {
	log.Printf("Retrieving metadata for file: %s", req.FileId)
	meta, err := s.store.Get(req.FileId)
	if err != nil {
		log.Printf("Failed to retrieve metadata for file %s: %v", req.FileId, err)
		return nil, status.Errorf(codes.NotFound, "metadata not found: %v", err)
	}

	log.Printf("Metadata retrieved successfully for file: %s", req.FileId)
	return &pb.GetFileMetadataResponse{
		Metadata: &pb.FileMetadata{
			FileId:    meta.FileID,
			FileName:  meta.FileName,
			FileSize:  meta.FileSize,
			ChunkIds:  meta.ChunkIDs,
			CreatedAt: meta.CreatedAt,
			UpdatedAt: meta.UpdatedAt,
		},
	}, nil
}

func (s *Server) DeleteFileMetadata(ctx context.Context, req *pb.DeleteFileMetadataRequest) (*pb.DeleteFileMetadataResponse, error) {
	log.Printf("Deleting metadata for file: %s", req.FileId)
	err := s.store.Delete(req.FileId)
	if err != nil {
		log.Printf("Failed to delete metadata for file %s: %v", req.FileId, err)
		return nil, status.Errorf(codes.Internal, "failed to delete metadata: %v", err)
	}

	log.Printf("Metadata deleted successfully for file: %s", req.FileId)
	return &pb.DeleteFileMetadataResponse{Success: true}, nil
}
