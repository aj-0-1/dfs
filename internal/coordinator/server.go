package coordinator

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"time"

	pbcoord "dfs/internal/pb/coordinator"
	pbmeta "dfs/internal/pb/metadata"
	pbstorage "dfs/internal/pb/storagenode"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
    chunkSize         = 64 * 1024 // 64 KB
    replicationFactor = 3
)

type Server struct {
    pbcoord.UnimplementedCoordinatorServer
    metadataClient pbmeta.MetadataServiceClient
    storageNodes   []pbstorage.StorageNodeClient
}

func NewServer(metadataAddr string, storageAddrs []string) (*Server, error) {
    ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
    defer cancel()

    metadataConn, err := grpc.DialContext(ctx, metadataAddr, grpc.WithInsecure(), grpc.WithBlock())
    if err != nil {
        return nil, fmt.Errorf("failed to connect to metadata service: %v", err)
    }

    metadataClient := pbmeta.NewMetadataServiceClient(metadataConn)

    var storageNodes []pbstorage.StorageNodeClient
    for _, addr := range storageAddrs {
        ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
        defer cancel()

        conn, err := grpc.DialContext(ctx, addr, grpc.WithInsecure(), grpc.WithBlock())
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
            log.Printf("Failed to receive chunk: %v", err)
            return status.Errorf(codes.Internal, "failed to receive chunk: %v", err)
        }

        if fileName == "" {
            fileName = req.GetFileName()
            fileID = generateFileID(fileName)
            log.Printf("Starting upload for file: %s (ID: %s)", fileName, fileID)
        }

        chunkID := generateChunkID(fileID, len(chunks))
        chunks = append(chunks, chunkID)

        checksum := calculateChecksum(req.GetChunkData())
        log.Printf("Calculated checksum for chunk %s: %s", chunkID, checksum)

        // Replicate chunk across multiple nodes
        for i := 0; i < replicationFactor; i++ {
            nodeIndex := (len(chunks) + i) % len(s.storageNodes)
            _, err = s.storageNodes[nodeIndex].PutChunk(context.Background(), &pbstorage.PutChunkRequest{
                ChunkId:  chunkID,
                Data:     req.GetChunkData(),
                Checksum: checksum,
            })
            if err != nil {
                log.Printf("Failed to store chunk %s on node %d: %v", chunkID, nodeIndex, err)
                return status.Errorf(codes.Internal, "failed to store chunk: %v", err)
            }
            log.Printf("Stored chunk %s on node %d", chunkID, nodeIndex)
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
        log.Printf("Failed to save metadata for file %s: %v", fileID, err)
        return status.Errorf(codes.Internal, "failed to save metadata: %v", err)
    }

    log.Printf("File uploaded successfully. File ID: %s, Size: %d bytes, Chunks: %d", fileID, fileSize, len(chunks))
    return stream.SendAndClose(&pbcoord.UploadFileResponse{
        FileId: fileID,
    })
}

func (s *Server) DownloadFile(req *pbcoord.DownloadFileRequest, stream pbcoord.Coordinator_DownloadFileServer) error {
    log.Printf("Starting download for file ID: %s", req.GetFileId())

    // Get metadata
    metaResp, err := s.metadataClient.GetFileMetadata(context.Background(), &pbmeta.GetFileMetadataRequest{
        FileId: req.GetFileId(),
    })
    if err != nil {
        log.Printf("Failed to retrieve metadata for file %s: %v", req.GetFileId(), err)
        return status.Errorf(codes.NotFound, "file not found: %v", err)
    }

    log.Printf("Retrieved metadata for file %s: %+v", req.GetFileId(), metaResp.Metadata)

    // Retrieve and stream chunks
    for i, chunkID := range metaResp.Metadata.ChunkIds {
        nodeIndex := i % len(s.storageNodes)
        chunkResp, err := s.storageNodes[nodeIndex].GetChunk(context.Background(), &pbstorage.GetChunkRequest{
            ChunkId: chunkID,
        })
        if err != nil {
            log.Printf("Failed to retrieve chunk %s from node %d: %v", chunkID, nodeIndex, err)
            // Try other replicas if the first attempt fails
            for j := 1; j < replicationFactor; j++ {
                nodeIndex = (i + j) % len(s.storageNodes)
                chunkResp, err = s.storageNodes[nodeIndex].GetChunk(context.Background(), &pbstorage.GetChunkRequest{
                    ChunkId: chunkID,
                })
                if err == nil {
                    break
                }
                log.Printf("Failed to retrieve chunk %s from node %d (attempt %d): %v", chunkID, nodeIndex, j+1, err)
            }
            if err != nil {
                return status.Errorf(codes.Internal, "failed to retrieve chunk: %v", err)
            }
        }

        // Verify checksum
        calculatedChecksum := calculateChecksum(chunkResp.Data)
        log.Printf("Chunk %s - Stored checksum: %s, Calculated checksum: %s", chunkID, chunkResp.Checksum, calculatedChecksum)
        if calculatedChecksum != chunkResp.Checksum {
            log.Printf("Checksum mismatch for chunk %s", chunkID)
            return status.Errorf(codes.DataLoss, "chunk checksum mismatch for chunk %s", chunkID)
        }

        err = stream.Send(&pbcoord.DownloadFileResponse{
            FileName:  metaResp.Metadata.FileName,
            ChunkData: chunkResp.Data,
        })
        if err != nil {
            log.Printf("Failed to send chunk %s to client: %v", chunkID, err)
            return status.Errorf(codes.Internal, "failed to send chunk: %v", err)
        }
        log.Printf("Sent chunk %s to client", chunkID)
    }

    log.Printf("File download completed successfully for file ID: %s", req.GetFileId())
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

    // Delete chunks from all replicas
    for i, chunkID := range metaResp.Metadata.ChunkIds {
        for j := 0; j < replicationFactor; j++ {
            nodeIndex := (i + j) % len(s.storageNodes)
            _, err := s.storageNodes[nodeIndex].DeleteChunk(ctx, &pbstorage.DeleteChunkRequest{
                ChunkId: chunkID,
            })
            if err != nil {
                // Log the error but continue with deletion
                fmt.Printf("Failed to delete chunk %s from node %d: %v\n", chunkID, nodeIndex, err)
            }
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

func calculateChecksum(data []byte) string {
    hash := sha256.Sum256(data)
    return hex.EncodeToString(hash[:])
}
