package metadataservice

import (
	"context"
	pb "dfs/internal/pb/metadata"
	"os"
	"testing"
)

func TestServer(t *testing.T) {
	// Create a temporary directory for testing
	tempDir, err := os.MkdirTemp("", "dfs-metadata-test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	store, err := NewDiskStore(tempDir)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}

	server := NewServer(store)

	// Test SaveFileMetadata
	saveReq := &pb.SaveFileMetadataRequest{
		Metadata: &pb.FileMetadata{
			FileId:   "test1",
			FileName: "test.txt",
			FileSize: 1000,
			ChunkIds: []string{"chunk1", "chunk2"},
		},
	}
	_, err = server.SaveFileMetadata(context.Background(), saveReq)
	if err != nil {
		t.Fatalf("SaveFileMetadata failed: %v", err)
	}

	// Test GetFileMetadata
	getReq := &pb.GetFileMetadataRequest{FileId: "test1"}
	getResp, err := server.GetFileMetadata(context.Background(), getReq)
	if err != nil {
		t.Fatalf("GetFileMetadata failed: %v", err)
	}
	if getResp.Metadata.FileName != "test.txt" {
		t.Fatalf("GetFileMetadata returned unexpected filename: %s", getResp.Metadata.FileName)
	}

	// Test UpdateFileMetadata
	updateReq := &pb.UpdateFileMetadataRequest{
		Metadata: &pb.FileMetadata{
			FileId:   "test1",
			FileName: "updated.txt",
			FileSize: 2000,
			ChunkIds: []string{"chunk1", "chunk2", "chunk3"},
		},
	}
	_, err = server.UpdateFileMetadata(context.Background(), updateReq)
	if err != nil {
		t.Fatalf("UpdateFileMetadata failed: %v", err)
	}

	// Verify update
	getResp, err = server.GetFileMetadata(context.Background(), getReq)
	if err != nil {
		t.Fatalf("GetFileMetadata failed after update: %v", err)
	}
	if getResp.Metadata.FileName != "updated.txt" {
		t.Fatalf("UpdateFileMetadata did not update filename: %s", getResp.Metadata.FileName)
	}

	// Test DeleteFileMetadata
	deleteReq := &pb.DeleteFileMetadataRequest{FileId: "test1"}
	_, err = server.DeleteFileMetadata(context.Background(), deleteReq)
	if err != nil {
		t.Fatalf("DeleteFileMetadata failed: %v", err)
	}

	// Verify deletion
	_, err = server.GetFileMetadata(context.Background(), getReq)
	if err == nil {
		t.Fatalf("GetFileMetadata should have failed for deleted metadata")
	}
}
