package udm

import (
	"context"
	"fmt"
	"io"
	"os"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/seaweedfs/seaweedfs/weed/storage/backend/udm/api"
)

type ClientSet struct {
	conn *grpc.ClientConn

	storageClient pb.UDMStorageClient
}

func NewClient(target string) (*ClientSet, error) {
	conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}

	return &ClientSet{
		conn:          conn,
		storageClient: pb.NewUDMStorageClient(conn),
	}, nil
}

func (cs *ClientSet) Close() error {
	if cs.conn != nil {
		return cs.conn.Close()
	}
	return nil
}

func (cs *ClientSet) UploadFile(ctx context.Context, filePath, key string, fn func(progressed int64, percentage float32) error) (int64, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stream, err := cs.storageClient.UploadFile(ctx, &pb.FileRequest{
		Key:  key,
		File: filePath,
	})
	if err != nil {
		return 0, err
	}

	var totalBytes int64
	for {
		select {
		case <-ctx.Done():
			return totalBytes, fmt.Errorf("context is canceled, err: %w", ctx.Err())
		default:
		}

		var res *pb.FileInfo
		res, err = stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return totalBytes, err
		}

		totalBytes = res.TotalBytes
		if fn != nil {
			err = fn(res.TotalBytes, res.Percentage)
			if err != nil {
				return totalBytes, err
			}
		}
	}

	return totalBytes, nil
}

func (cs *ClientSet) DownloadFile(ctx context.Context, filePath, key string, fn func(progressed int64, percentage float32) error) (int64, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stream, err := cs.storageClient.DownloadFile(ctx, &pb.FileRequest{
		Key:  key,
		File: filePath,
	})

	if err != nil {
		return 0, err
	}

	var totalBytes int64
	for {
		select {
		case <-ctx.Done():
			return totalBytes, fmt.Errorf("context is canceled, err: %w", ctx.Err())
		default:
		}

		var res *pb.FileInfo
		res, err = stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return totalBytes, err
		}

		totalBytes = res.TotalBytes
		if fn != nil {
			err = fn(res.TotalBytes, res.Percentage)
			if err != nil {
				return totalBytes, err
			}
		}
	}

	return totalBytes, nil
}

func (cs *ClientSet) DeleteFile(ctx context.Context, key string) error {
	_, err := cs.storageClient.DeleteFile(ctx, &pb.FileKey{
		Key: key,
	})

	return err
}

func (cs *ClientSet) ReadAt(ctx context.Context, key string, offset, length int64) ([]byte, error) {
	res, err := cs.storageClient.CacheFile(ctx, &pb.FileKey{
		Key: key,
	})
	if err != nil {
		return nil, err
	}

	f, err := os.Open(res.CacheFile)
	if err != nil {
		return nil, err
	}

	defer f.Close()

	buffer := make([]byte, length)
	_, err = f.ReadAt(buffer, offset)
	if err != nil {
		return nil, err
	}

	return buffer, nil
}
