package chunk

import (
	"fmt"
	"os"
	"path/filepath"
)

type Store interface {
	Put(id string, data []byte, checksum string) error
	Get(id string) ([]byte, string, error)
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

func (d *DiskStore) Put(id string, data []byte, checksum string) error {
	path := filepath.Join(d.baseDir, id)
	if err := os.WriteFile(path, data, 0644); err != nil {
		return err
	}
	checksumPath := filepath.Join(d.baseDir, id+".checksum")

	if err := os.WriteFile(checksumPath, []byte(checksum), 0644); err != nil {
		return err
	}
	return nil
}

func (d *DiskStore) Get(id string) ([]byte, string, error) {
	path := filepath.Join(d.baseDir, id)
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, "", err
	}
	checksumPath := filepath.Join(d.baseDir, id+".checksum")
	checksum, err := os.ReadFile(checksumPath)
	if err != nil {
		return nil, "", err
	}
	return data, string(checksum), nil
}

func (d *DiskStore) Delete(id string) error {
	path := filepath.Join(d.baseDir, id)
	if err := os.Remove(path); err != nil {
		return err
	}
	checksumPath := filepath.Join(d.baseDir, id+".checksum")

	if err := os.Remove(checksumPath); err != nil {
		return err
	}
	return nil
}
