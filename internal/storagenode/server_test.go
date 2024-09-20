package storagenode

import (
	"context"
	"dfs/internal/chunk"
	pb "dfs/internal/pb/storagenode"
	"os"
	"testing"
)

func TestServer(t *testing.T) {
	// Create a temporary directory for testing
	tempDir, err := os.MkdirTemp("", "dfs-test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	store, err := chunk.NewDiskStore(tempDir)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}

	server := NewServer(store)

	// Test PutChunk
	putReq := &pb.PutChunkRequest{ChunkId: "test1", Data: []byte("hello world")}
	_, err = server.PutChunk(context.Background(), putReq)
	if err != nil {
		t.Fatalf("PutChunk failed: %v", err)
	}

	// Test GetChunk
	getReq := &pb.GetChunkRequest{ChunkId: "test1"}
	getResp, err := server.GetChunk(context.Background(), getReq)
	if err != nil {
		t.Fatalf("GetChunk failed: %v", err)
	}
	if string(getResp.Data) != "hello world" {
		t.Fatalf("GetChunk returned unexpected data: %s", getResp.Data)
	}

	// Test DeleteChunk
	deleteReq := &pb.DeleteChunkRequest{ChunkId: "test1"}
	_, err = server.DeleteChunk(context.Background(), deleteReq)
	if err != nil {
		t.Fatalf("DeleteChunk failed: %v", err)
	}

	// Verify chunk is deleted
	_, err = server.GetChunk(context.Background(), getReq)
	if err == nil {
		t.Fatalf("GetChunk should have failed for deleted chunk")
	}
}
