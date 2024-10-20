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
		Chunks:    make([]ChunkInfo, len(req.Metadata.Chunks)),
		CreatedAt: time.Now().UTC().Format(time.RFC3339),
		UpdatedAt: time.Now().UTC().Format(time.RFC3339),
	}

	for i, chunk := range req.Metadata.Chunks {
		meta.Chunks[i] = ChunkInfo{
			ChunkID: chunk.ChunkId,
			NodeIDs: chunk.NodeIds,
		}
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

	pbMeta := &pb.FileMetadata{
		FileId:    meta.FileID,
		FileName:  meta.FileName,
		FileSize:  meta.FileSize,
		Chunks:    make([]*pb.ChunkInfo, len(meta.Chunks)),
		CreatedAt: meta.CreatedAt,
		UpdatedAt: meta.UpdatedAt,
	}

	for i, chunk := range meta.Chunks {
		pbMeta.Chunks[i] = &pb.ChunkInfo{
			ChunkId: chunk.ChunkID,
			NodeIds: chunk.NodeIDs,
		}
	}

	log.Printf("Metadata retrieved successfully for file: %s", req.FileId)
	return &pb.GetFileMetadataResponse{Metadata: pbMeta}, nil
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
