package metadataservice

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

type ChunkInfo struct {
	ChunkID string
	NodeIDs []string
}

type FileMetadata struct {
	FileID    string
	FileName  string
	FileSize  int64
	Chunks    []ChunkInfo
	CreatedAt string
	UpdatedAt string
}

type Store interface {
	Save(metadata *FileMetadata) error
	Get(fileID string) (*FileMetadata, error)
	Delete(fileID string) error
}

type DiskStore struct {
	baseDir string
	mu      sync.RWMutex
}

func NewDiskStore(baseDir string) (*DiskStore, error) {
	if err := os.MkdirAll(baseDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create base directory: %w", err)
	}
	return &DiskStore{baseDir: baseDir}, nil
}

func (d *DiskStore) Save(metadata *FileMetadata) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	data, err := json.Marshal(metadata)
	if err != nil {
		return fmt.Errorf("failed to marshal metadata: %w", err)
	}

	path := filepath.Join(d.baseDir, metadata.FileID+".json")
	if err := os.WriteFile(path, data, 0644); err != nil {
		return fmt.Errorf("failed to write metadata to file: %w", err)
	}

	return nil
}

func (d *DiskStore) Get(fileID string) (*FileMetadata, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	path := filepath.Join(d.baseDir, fileID+".json")
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read metadata file: %w", err)
	}

	var metadata FileMetadata
	if err := json.Unmarshal(data, &metadata); err != nil {
		return nil, fmt.Errorf("failed to unmarshal metadata: %w", err)
	}

	return &metadata, nil
}

func (d *DiskStore) Delete(fileID string) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	path := filepath.Join(d.baseDir, fileID+".json")
	if err := os.Remove(path); err != nil {
		return err
	}
	return nil
}

func (d *DiskStore) List() ([]*FileMetadata, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	files, err := os.ReadDir(d.baseDir)
	if err != nil {
		return nil, fmt.Errorf("failed to read directory: %w", err)
	}

	var metadataList []*FileMetadata
	for _, file := range files {
		if filepath.Ext(file.Name()) != ".json" {
			continue
		}

		metadata, err := d.Get(filepath.Base(file.Name()[:len(file.Name())-5]))
		if err != nil {
			return nil, fmt.Errorf("failed to get metadata for file %s: %w", file.Name(), err)
		}

		metadataList = append(metadataList, metadata)
	}

	return metadataList, nil
}
