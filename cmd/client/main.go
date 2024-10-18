package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	pbcoord "dfs/internal/pb/coordinator"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	options := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}

	conn, err := grpc.NewClient("localhost:50053", options...)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer conn.Close()

	client := pbcoord.NewCoordinatorClient(conn)

	reader := bufio.NewReader(os.Stdin)

	for {
		fmt.Print("Enter command (upload/download/delete/exit): ")
		command, _ := reader.ReadString('\n')
		command = strings.TrimSpace(command)

		switch command {
		case "upload":
			uploadFile(client, reader)
		case "download":
			downloadFile(client, reader)
		case "delete":
			deleteFile(client, reader)
		case "exit":
			return
		default:
			fmt.Println("Unknown command")
		}
	}
}

func uploadFile(client pbcoord.CoordinatorClient, reader *bufio.Reader) {
	fmt.Print("Enter file path: ")
	filePath, err := reader.ReadString('\n')
	if err != nil {
		log.Printf("Failed to read string: %v", err)
		return
	}
	filePath = strings.TrimSpace(filePath)

	file, err := os.Open(filePath)
	if err != nil {
		log.Printf("Failed to open file: %v", err)
		return
	}
	defer file.Close()

	fileName := filepath.Base(filePath)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	stream, err := client.UploadFile(ctx)
	if err != nil {
		log.Printf("Failed to start upload: %v", err)
		return
	}

	buffer := make([]byte, 64*1024)
	for {
		n, err := file.Read(buffer)
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("Failed to read file: %v", err)
			return
		}

		err = stream.Send(&pbcoord.UploadFileRequest{
			FileName:  fileName,
			ChunkData: buffer[:n],
		})
		if err != nil {
			log.Printf("Failed to send chunk: %v", err)
			return
		}
	}

	resp, err := stream.CloseAndRecv()
	if err != nil {
		log.Printf("Failed to complete upload: %v", err)
		return
	}

	fmt.Printf("File uploaded successfully. File ID: %s\n", resp.FileId)
}

func downloadFile(client pbcoord.CoordinatorClient, reader *bufio.Reader) {
	fmt.Print("Enter file ID: ")
	fileID, err := reader.ReadString('\n')
	if err != nil {
		log.Printf("Failed to read file ID: %v", err)
		return
	}
	fileID = strings.TrimSpace(fileID)

	homeDir, err := os.UserHomeDir()
	if err != nil {
		log.Printf("Failed to get user's home directory: %v", err)
		return
	}

	defaultDownloadDir := filepath.Join(homeDir, "Downloads", "DFS")

	fmt.Printf("Enter download path (press Enter for default: %s): ", defaultDownloadDir)
	downloadPath, err := reader.ReadString('\n')
	if err != nil {
		log.Printf("Failed to read download path: %v", err)
		return
	}
	downloadPath = strings.TrimSpace(downloadPath)
	if downloadPath == "" {
		downloadPath = defaultDownloadDir
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	stream, err := client.DownloadFile(ctx, &pbcoord.DownloadFileRequest{FileId: fileID})
	if err != nil {
		log.Printf("Failed to start download: %v", err)
		return
	}

	var fileName string
	var fileData []byte
	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("Failed to receive chunk: %v", err)
			return
		}
		if fileName == "" {
			fileName = resp.FileName
		}
		fileData = append(fileData, resp.ChunkData...)
	}

	fullPath := filepath.Join(downloadPath, fileName)
	err = os.MkdirAll(filepath.Dir(fullPath), 0755)
	if err != nil {
		log.Printf("Failed to create directory: %v", err)
		return
	}

	err = os.WriteFile(fullPath, fileData, 0644)
	if err != nil {
		log.Printf("Failed to write file: %v", err)
		return
	}

	fmt.Printf("File downloaded successfully: %s\n", fullPath)
}

func deleteFile(client pbcoord.CoordinatorClient, reader *bufio.Reader) {
	fmt.Print("Enter file ID: ")
	fileID, err := reader.ReadString('\n')
	if err != nil {
		log.Printf("Failed to read string: %v", err)
		return
	}
	fileID = strings.TrimSpace(fileID)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	resp, err := client.DeleteFile(ctx, &pbcoord.DeleteFileRequest{FileId: fileID})
	if err != nil {
		log.Printf("Failed to delete file: %v", err)
		return
	}

	if resp.Success {
		fmt.Println("File deleted successfully")
	} else {
		fmt.Println("Failed to delete file")
	}
}
