package coordinator

import (
	"context"
	"dfs/internal/pb/coordinator"
	"dfs/internal/pb/metadata"
	"dfs/internal/pb/storagenode"
	"io"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Mock MetadataServiceClient
type mockMetadataServiceClient struct {
	metadata.MetadataServiceClient
	savedMetadata *metadata.FileMetadata
}

func (m *mockMetadataServiceClient) SaveFileMetadata(ctx context.Context, in *metadata.SaveFileMetadataRequest, opts ...grpc.CallOption) (*metadata.SaveFileMetadataResponse, error) {
	m.savedMetadata = in.Metadata
	return &metadata.SaveFileMetadataResponse{Success: true}, nil
}

func (m *mockMetadataServiceClient) GetFileMetadata(ctx context.Context, in *metadata.GetFileMetadataRequest, opts ...grpc.CallOption) (*metadata.GetFileMetadataResponse, error) {
	return &metadata.GetFileMetadataResponse{Metadata: m.savedMetadata}, nil
}

// Mock StorageNodeClient
type mockStorageNodeClient struct {
	storagenode.StorageNodeClient
	storedChunks map[string][]byte
}

func newMockStorageNodeClient() *mockStorageNodeClient {
	return &mockStorageNodeClient{storedChunks: make(map[string][]byte)}
}

func (m *mockStorageNodeClient) PutChunk(ctx context.Context, in *storagenode.PutChunkRequest, opts ...grpc.CallOption) (*storagenode.PutChunkResponse, error) {
	m.storedChunks[in.ChunkId] = in.Data
	return &storagenode.PutChunkResponse{Success: true}, nil
}

func (m *mockStorageNodeClient) GetChunk(ctx context.Context, in *storagenode.GetChunkRequest, opts ...grpc.CallOption) (*storagenode.GetChunkResponse, error) {
	data, ok := m.storedChunks[in.ChunkId]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "Chunk not found")
	}
	return &storagenode.GetChunkResponse{Data: data}, nil
}

// Mock UploadFile_Server
type mockUploadFileServer struct {
	grpc.ServerStream
	ctx     context.Context
	recvMsg []byte
	sentMsg *coordinator.UploadFileResponse
}

func (m *mockUploadFileServer) Context() context.Context {
	return m.ctx
}

func (m *mockUploadFileServer) SendAndClose(r *coordinator.UploadFileResponse) error {
	m.sentMsg = r
	return nil
}

func (m *mockUploadFileServer) Recv() (*coordinator.UploadFileRequest, error) {
	if m.recvMsg == nil {
		return nil, io.EOF
	}
	req := &coordinator.UploadFileRequest{
		FileName:  "test.txt",
		ChunkData: m.recvMsg,
	}
	m.recvMsg = nil
	return req, nil
}

func TestCoordinator_UploadFile(t *testing.T) {
	metadataClient := &mockMetadataServiceClient{}
	storageClient := newMockStorageNodeClient()

	server := &Server{
		metadataClient: metadataClient,
		storageNodes:   []storagenode.StorageNodeClient{storageClient},
	}

	uploadServer := &mockUploadFileServer{
		ctx:     context.Background(),
		recvMsg: []byte("test data"),
	}

	err := server.UploadFile(uploadServer)
	if err != nil {
		t.Fatalf("UploadFile failed: %v", err)
	}

	if uploadServer.sentMsg == nil {
		t.Fatal("No response sent")
	}

	if uploadServer.sentMsg.FileId == "" {
		t.Fatal("FileId is empty")
	}

	if metadataClient.savedMetadata == nil {
		t.Fatal("Metadata not saved")
	}

	if len(storageClient.storedChunks) != 1 {
		t.Fatalf("Expected 1 chunk, got %d", len(storageClient.storedChunks))
	}
}
