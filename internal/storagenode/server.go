package storagenode

import (
	"context"
	"dfs/internal/chunk"
	pb "dfs/internal/pb/storagenode"
	"log"

	"github.com/google/uuid"
)

type Server struct {
	pb.UnimplementedStorageNodeServer
	store  chunk.Store
	nodeID string
}

func NewServer(store chunk.Store) *Server {
	return &Server{
		store:  store,
		nodeID: uuid.New().String(),
	}
}

func (s *Server) GetNodeID(ctx context.Context, req *pb.GetNodeIDRequest) (*pb.GetNodeIDResponse, error) {
	return &pb.GetNodeIDResponse{NodeId: s.nodeID}, nil
}

func (s *Server) PutChunk(ctx context.Context, req *pb.PutChunkRequest) (*pb.PutChunkResponse, error) {
	log.Printf("Storing chunk: %s with checksum: %s", req.ChunkId, req.Checksum)
	err := s.store.Put(req.ChunkId, req.Data, req.Checksum)
	if err != nil {
		log.Printf("Failed to store chunk %s: %v", req.ChunkId, err)
		return nil, err
	}
	log.Printf("Chunk stored successfully: %s", req.ChunkId)
	return &pb.PutChunkResponse{Success: true}, nil
}

func (s *Server) GetChunk(ctx context.Context, req *pb.GetChunkRequest) (*pb.GetChunkResponse, error) {
	log.Printf("Retrieving chunk: %s", req.ChunkId)
	data, checksum, err := s.store.Get(req.ChunkId)
	if err != nil {
		log.Printf("Failed to retrieve chunk %s: %v", req.ChunkId, err)
		return nil, err
	}
	log.Printf("Chunk retrieved successfully: %s with checksum: %s", req.ChunkId, checksum)
	return &pb.GetChunkResponse{Data: data, Checksum: checksum}, nil
}

func (s *Server) DeleteChunk(ctx context.Context, req *pb.DeleteChunkRequest) (*pb.DeleteChunkResponse, error) {
	log.Printf("Deleting chunk: %s", req.ChunkId)
	err := s.store.Delete(req.ChunkId)
	if err != nil {
		log.Printf("Failed to delete chunk %s: %v", req.ChunkId, err)
		return nil, err
	}
	log.Printf("Chunk deleted successfully: %s", req.ChunkId)
	return &pb.DeleteChunkResponse{Success: true}, nil
}
