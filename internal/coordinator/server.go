package coordinator

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"time"

	pbcoord "dfs/internal/pb/coordinator"
	pbmeta "dfs/internal/pb/metadata"
	pbstorage "dfs/internal/pb/storagenode"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const chunkSize = 64 * 1024 // 64 KB

type Server struct {
    pbcoord.UnimplementedCoordinatorServer
    metadataClient pbmeta.MetadataServiceClient
    storageNodes   []pbstorage.StorageNodeClient
}

func NewServer(metadataAddr string, storageAddrs []string) (*Server, error) {
    metadataConn, err := grpc.Dial(metadataAddr, grpc.WithInsecure())
    if err != nil {
        return nil, fmt.Errorf("failed to connect to metadata service: %v", err)
    }

    metadataClient := pbmeta.NewMetadataServiceClient(metadataConn)

    var storageNodes []pbstorage.StorageNodeClient
    for _, addr := range storageAddrs {
        conn, err := grpc.Dial(addr, grpc.WithInsecure())
        if err != nil {
            return nil, fmt.Errorf("failed to connect to storage node %s: %v", addr, err)
        }
        storageNodes = append(storageNodes, pbstorage.NewStorageNodeClient(conn))
    }

    return &Server{
        metadataClient: metadataClient,
        storageNodes:   storageNodes,
    }, nil
}

func (s *Server) UploadFile(stream pbcoord.Coordinator_UploadFileServer) error {
    var fileID, fileName string
    var fileSize int64
    var chunks []string

    for {
        req, err := stream.Recv()
        if err == io.EOF {
            break
        }
        if err != nil {
            return status.Errorf(codes.Internal, "failed to receive chunk: %v", err)
        }

        if fileName == "" {
            fileName = req.GetFileName()
            fileID = generateFileID(fileName)
        }

        chunkID := generateChunkID(fileID, len(chunks))
        chunks = append(chunks, chunkID)

        // Distribute chunk to a storage node (using round-robin for simplicity)
        nodeIndex := len(chunks) % len(s.storageNodes)
        _, err = s.storageNodes[nodeIndex].PutChunk(context.Background(), &pbstorage.PutChunkRequest{
            ChunkId: chunkID,
            Data:    req.GetChunkData(),
        })
        if err != nil {
            return status.Errorf(codes.Internal, "failed to store chunk: %v", err)
        }

        fileSize += int64(len(req.GetChunkData()))
    }

    // Save metadata
    _, err := s.metadataClient.SaveFileMetadata(context.Background(), &pbmeta.SaveFileMetadataRequest{
        Metadata: &pbmeta.FileMetadata{
            FileId:    fileID,
            FileName:  fileName,
            FileSize:  fileSize,
            ChunkIds:  chunks,
            CreatedAt: time.Now().UTC().Format(time.RFC3339),
            UpdatedAt: time.Now().UTC().Format(time.RFC3339),
        },
    })
    if err != nil {
        return status.Errorf(codes.Internal, "failed to save metadata: %v", err)
    }

    return stream.SendAndClose(&pbcoord.UploadFileResponse{
        FileId: fileID,
    })
}

func (s *Server) DownloadFile(req *pbcoord.DownloadFileRequest, stream pbcoord.Coordinator_DownloadFileServer) error {
    // Get metadata
    metaResp, err := s.metadataClient.GetFileMetadata(context.Background(), &pbmeta.GetFileMetadataRequest{
        FileId: req.GetFileId(),
    })
    if err != nil {
        return status.Errorf(codes.NotFound, "file not found: %v", err)
    }

    // Retrieve and stream chunks
    for i, chunkID := range metaResp.Metadata.ChunkIds {
        nodeIndex := i % len(s.storageNodes)
        chunkResp, err := s.storageNodes[nodeIndex].GetChunk(context.Background(), &pbstorage.GetChunkRequest{
            ChunkId: chunkID,
        })
        if err != nil {
            return status.Errorf(codes.Internal, "failed to retrieve chunk: %v", err)
        }

        err = stream.Send(&pbcoord.DownloadFileResponse{
            FileName:  metaResp.Metadata.FileName,
            ChunkData: chunkResp.Data,
        })
        if err != nil {
            return status.Errorf(codes.Internal, "failed to send chunk: %v", err)
        }
    }

    return nil
}

func (s *Server) DeleteFile(ctx context.Context, req *pbcoord.DeleteFileRequest) (*pbcoord.DeleteFileResponse, error) {
    // Get metadata
    metaResp, err := s.metadataClient.GetFileMetadata(ctx, &pbmeta.GetFileMetadataRequest{
        FileId: req.GetFileId(),
    })
    if err != nil {
        return nil, status.Errorf(codes.NotFound, "file not found: %v", err)
    }

    // Delete chunks
    for i, chunkID := range metaResp.Metadata.ChunkIds {
        nodeIndex := i % len(s.storageNodes)
        _, err := s.storageNodes[nodeIndex].DeleteChunk(ctx, &pbstorage.DeleteChunkRequest{
            ChunkId: chunkID,
        })
        if err != nil {
            return nil, status.Errorf(codes.Internal, "failed to delete chunk: %v", err)
        }
    }

    // Delete metadata
    _, err = s.metadataClient.DeleteFileMetadata(ctx, &pbmeta.DeleteFileMetadataRequest{
        FileId: req.GetFileId(),
    })
    if err != nil {
        return nil, status.Errorf(codes.Internal, "failed to delete metadata: %v", err)
    }

    return &pbcoord.DeleteFileResponse{Success: true}, nil
}

func generateFileID(fileName string) string {
    hash := sha256.Sum256([]byte(fileName + time.Now().String()))
    return fmt.Sprintf("%x", hash[:8])
}

func generateChunkID(fileID string, chunkIndex int) string {
    return fmt.Sprintf("%s-chunk-%d", fileID, chunkIndex)
}
