package metadataservice

import (
	"context"
	pb "dfs/internal/pb/metadata"
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
	meta := &FileMetadata{
		FileID:    req.Metadata.FileId,
		FileName:  req.Metadata.FileName,
		FileSize:  req.Metadata.FileSize,
		ChunkIDs:  req.Metadata.ChunkIds,
		CreatedAt: time.Now().UTC().Format(time.RFC3339),
		UpdatedAt: time.Now().UTC().Format(time.RFC3339),
	}

	if err := s.store.Save(meta); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to save metadata: %v", err)
	}

	return &pb.SaveFileMetadataResponse{Success: true}, nil
}

func (s *Server) GetFileMetadata(ctx context.Context, req *pb.GetFileMetadataRequest) (*pb.GetFileMetadataResponse, error) {
	meta, err := s.store.Get(req.FileId)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "metadata not found: %v", err)
	}

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
	if err := s.store.Delete(req.FileId); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to delete metadata: %v", err)
	}

	return &pb.DeleteFileMetadataResponse{Success: true}, nil
}

func (s *Server) UpdateFileMetadata(ctx context.Context, req *pb.UpdateFileMetadataRequest) (*pb.UpdateFileMetadataResponse, error) {
	meta, err := s.store.Get(req.Metadata.FileId)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "metadata not found: %v", err)
	}

	meta.FileName = req.Metadata.FileName
	meta.FileSize = req.Metadata.FileSize
	meta.ChunkIDs = req.Metadata.ChunkIds
	meta.UpdatedAt = time.Now().UTC().Format(time.RFC3339)

	if err := s.store.Save(meta); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to update metadata: %v", err)
	}

	return &pb.UpdateFileMetadataResponse{Success: true}, nil
}
