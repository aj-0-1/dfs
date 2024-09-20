package chunk

import (
	"fmt"
	"os"
	"path/filepath"
)

type Store interface {
	Put(id string, data []byte) error
	Get(id string) ([]byte, error)
	Delete(id string) error
}

type DiskStore struct {
	baseDir string
}

func NewDiskStore(baseDir string) (*DiskStore, error) {
	if err := os.MkdirAll(baseDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create base directory: %w", err)
	}
	return &DiskStore{baseDir: baseDir}, nil
}

func (d *DiskStore) Put(id string, data []byte) error {
	path := filepath.Join(d.baseDir, id)
	if err := os.WriteFile(path, data, 0644); err != nil {
		return fmt.Errorf("failed to write data chunk to base directory: %w", err)
	}
	return nil
}

func (d *DiskStore) Get(id string) ([]byte, error) {
	path := filepath.Join(d.baseDir, id)
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to get data chunk for id (%s): %w", id, err)
	}

	return data, nil
}

func (d *DiskStore) Delete(id string) error {
	path := filepath.Join(d.baseDir, id)
	if err := os.Remove(path); err != nil {
		return fmt.Errorf("failed to delete data chunk for id (%s): %w", id, err)
	}
	return nil
}
