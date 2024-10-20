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
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

const (
	chunkSize         = 64 * 1024 // 64 KB
	replicationFactor = 3
)

type Server struct {
	pbcoord.UnimplementedCoordinatorServer
	metadataClient pbmeta.MetadataServiceClient
	storageNodes   []StorageNode
}

type StorageNode struct {
	client pbstorage.StorageNodeClient
	nodeID string
}

func NewServer(metadataAddr string, storageAddrs []string) (*Server, error) {
	options := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}

	metadataConn, err := grpc.NewClient(metadataAddr, options...)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to metadata service: %v", err)
	}

	metadataClient := pbmeta.NewMetadataServiceClient(metadataConn)

	var storageNodes []StorageNode
	for _, addr := range storageAddrs {
		conn, err := grpc.NewClient(addr, options...)
		if err != nil {
			return nil, fmt.Errorf("failed to connect to storage node %s: %v", addr, err)
		}

		client := pbstorage.NewStorageNodeClient(conn)
		resp, err := client.GetNodeID(context.Background(), &pbstorage.GetNodeIDRequest{})
		if err != nil {
			return nil, fmt.Errorf("failed to get node ID for %s: %v", addr, err)
		}

		storageNode := StorageNode{client: client, nodeID: resp.NodeId}
		storageNodes = append(storageNodes, storageNode)
	}

	return &Server{
		metadataClient: metadataClient,
		storageNodes:   storageNodes,
	}, nil
}

func (s *Server) UploadFile(stream pbcoord.Coordinator_UploadFileServer) error {
	var fileID, fileName string
	var fileSize int64
	var chunkInfos []*pbmeta.ChunkInfo

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

		chunkID := generateChunkID(fileID, len(chunkInfos))
		chunkInfo := &pbmeta.ChunkInfo{
			ChunkId: chunkID,
			NodeIds: []string{},
		}

		checksum := calculateChecksum(req.GetChunkData())
		log.Printf("Calculated checksum for chunk %s: %s", chunkID, checksum)

		for i := 0; i < replicationFactor; i++ {
			nodeIndex := (len(chunkInfos) + i) % len(s.storageNodes)
			_, err = s.storageNodes[nodeIndex].client.PutChunk(context.Background(), &pbstorage.PutChunkRequest{
				ChunkId:  chunkID,
				Data:     req.GetChunkData(),
				Checksum: checksum,
			})
			if err != nil {
				log.Printf("Failed to store chunk %s on node %d: %v", chunkID, nodeIndex, err)
				return status.Errorf(codes.Internal, "failed to store chunk: %v", err)
			}

			chunkInfo.NodeIds = append(chunkInfo.NodeIds, s.storageNodes[nodeIndex].nodeID)
			log.Printf("Stored chunk %s on node %d", chunkID, nodeIndex)
		}

		chunkInfos = append(chunkInfos, chunkInfo)
		fileSize += int64(len(req.GetChunkData()))
	}

	_, err := s.metadataClient.SaveFileMetadata(context.Background(), &pbmeta.SaveFileMetadataRequest{
		Metadata: &pbmeta.FileMetadata{
			FileId:    fileID,
			FileName:  fileName,
			FileSize:  fileSize,
			Chunks:    chunkInfos,
			CreatedAt: time.Now().UTC().Format(time.RFC3339),
			UpdatedAt: time.Now().UTC().Format(time.RFC3339),
		},
	})
	if err != nil {
		log.Printf("Failed to save metadata for file %s: %v", fileID, err)
		return status.Errorf(codes.Internal, "failed to save metadata: %v", err)
	}

	log.Printf("File uploaded successfully. File ID: %s, Size: %d bytes, Chunks: %d", fileID, fileSize, len(chunkInfos))
	return stream.SendAndClose(&pbcoord.UploadFileResponse{
		FileId: fileID,
	})
}

func (s *Server) DownloadFile(req *pbcoord.DownloadFileRequest, stream pbcoord.Coordinator_DownloadFileServer) error {
	log.Printf("Starting download for file ID: %s", req.GetFileId())

	metaResp, err := s.metadataClient.GetFileMetadata(context.Background(), &pbmeta.GetFileMetadataRequest{
		FileId: req.GetFileId(),
	})
	if err != nil {
		log.Printf("Failed to retrieve metadata for file %s: %v", req.GetFileId(), err)
		return status.Errorf(codes.NotFound, "file not found: %v", err)
	}

	log.Printf("Retrieved metadata for file %s: %+v", req.GetFileId(), metaResp.Metadata)

	for _, chunkInfo := range metaResp.Metadata.Chunks {
		var chunkData []byte
		var chunkErr error

		for _, nodeID := range chunkInfo.NodeIds {
			var node *StorageNode
			for _, n := range s.storageNodes {
				if n.nodeID == nodeID {
					node = &n
					break
				}
			}
			if node == nil {
				log.Printf("Node %s not found for chunk %s", nodeID, chunkInfo.ChunkId)
				continue
			}

			chunkResp, err := node.client.GetChunk(context.Background(), &pbstorage.GetChunkRequest{
				ChunkId: chunkInfo.ChunkId,
			})
			if err != nil {
				log.Printf("Failed to retrieve chunk %s from node %s: %v", chunkInfo.ChunkId, nodeID, err)
				continue
			}

			calculatedChecksum := calculateChecksum(chunkResp.Data)
			if calculatedChecksum != chunkResp.Checksum {
				log.Printf("Checksum mismatch for chunk %s from node %s", chunkInfo.ChunkId, nodeID)
				continue
			}

			chunkData = chunkResp.Data
			break
		}

		if chunkData == nil {
			chunkErr = fmt.Errorf("failed to retrieve chunk %s from any node", chunkInfo.ChunkId)
		}

		if chunkErr != nil {
			return status.Errorf(codes.Internal, "failed to retrieve chunk: %v", chunkErr)
		}

		err = stream.Send(&pbcoord.DownloadFileResponse{
			FileName:  metaResp.Metadata.FileName,
			ChunkData: chunkData,
		})
		if err != nil {
			log.Printf("Failed to send chunk %s to client: %v", chunkInfo.ChunkId, err)
			return status.Errorf(codes.Internal, "failed to send chunk: %v", err)
		}
		log.Printf("Sent chunk %s to client", chunkInfo.ChunkId)
	}

	log.Printf("File download completed successfully for file ID: %s", req.GetFileId())
	return nil
}

func (s *Server) DeleteFile(ctx context.Context, req *pbcoord.DeleteFileRequest) (*pbcoord.DeleteFileResponse, error) {
	metaResp, err := s.metadataClient.GetFileMetadata(ctx, &pbmeta.GetFileMetadataRequest{
		FileId: req.GetFileId(),
	})
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "file not found: %v", err)
	}

	for _, chunkInfo := range metaResp.Metadata.Chunks {
		for _, nodeID := range chunkInfo.NodeIds {
			var node *StorageNode
			for _, n := range s.storageNodes {
				if n.nodeID == nodeID {
					node = &n
					break
				}
			}
			if node == nil {
				log.Printf("Node %s not found for chunk %s", nodeID, chunkInfo.ChunkId)
				continue
			}
			_, err := node.client.DeleteChunk(ctx, &pbstorage.DeleteChunkRequest{
				ChunkId: chunkInfo.ChunkId,
			})
			if err != nil {
				log.Printf("Failed to delete chunk %s from node %s: %v", chunkInfo.ChunkId, nodeID, err)
			}

			log.Printf("Deleted chunk %s from node %s", chunkInfo.ChunkId, nodeID)
		}
	}

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
