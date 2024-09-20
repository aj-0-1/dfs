package storagenode

import (
	"context"
	"dfs/internal/chunk"
	pb "dfs/internal/pb/storagenode"
)

type Server struct {
    pb.UnimplementedStorageNodeServer
    store chunk.Store
}

func NewServer(store chunk.Store) *Server {
    return &Server{store: store}
}

func (s *Server) PutChunk(ctx context.Context, req *pb.PutChunkRequest) (*pb.PutChunkResponse, error) {
    err := s.store.Put(req.ChunkId, req.Data)
    if err != nil {
        return nil, err
    }
    return &pb.PutChunkResponse{Success: true}, nil
}

func (s *Server) GetChunk(ctx context.Context, req *pb.GetChunkRequest) (*pb.GetChunkResponse, error) {
    data, err := s.store.Get(req.ChunkId)
    if err != nil {
        return nil, err
    }
    return &pb.GetChunkResponse{Data: data}, nil
}

func (s *Server) DeleteChunk(ctx context.Context, req *pb.DeleteChunkRequest) (*pb.DeleteChunkResponse, error) {
    err := s.store.Delete(req.ChunkId)
    if err != nil {
        return nil, err
    }
    return &pb.DeleteChunkResponse{Success: true}, nil
}

func (s *Server) GetStatus(ctx context.Context, req *pb.GetStatusRequest) (*pb.GetStatusResponse, error) {
    // Implement status checking logic here
    return &pb.GetStatusResponse{
        AvailableSpace: 1000000, // Example value
        IsHealthy:      true,
    }, nil
}
