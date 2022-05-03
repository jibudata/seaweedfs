package archival_client

import (
	"context"
	"log"
	"os"
	"time"

	pb "github.com/chrislusf/seaweedfs/weed/pb/archival"
	"google.golang.org/grpc"
)

type FrontApi struct {
	addr string
	c    pb.ArchivalerClient
}

type Pool struct {
	Poolname string
	Total    uint64
	Free     uint64
	Unref    uint64
	Numtapes uint64
}

type AsyncStatus struct {
	ReqNumber   uint64
	Success     bool
	Done        bool
	Resident    int64
	Transferred int64
	Premigrated int64
	Migrated    int64
	Failed      int64
}

type FileInfo struct {
	MigState   string
	FileName   string
	Size       uint64
	Blocks     uint64
	Fsidh      uint64
	Fsidl      uint64
	Igen       uint64
	Inum       uint64
	TapeId     string
	StartBlock uint64
}

func NewFrontApi(addr string) (front *FrontApi, e error) {
	front = &FrontApi{addr: addr}
	conn, err := grpc.Dial(addr, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
		e = err
		return
	}
	// defer conn.Close()
	client := pb.NewArchivalerClient(conn)
	front.c = client
	return
}

func (f *FrontApi) GetTapeInfo(tapeName string) (tapeInfo string, e error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(30)*time.Second)
	defer cancel()
	r, err := f.c.GetTapeInfo(ctx, &pb.TapeInfoRequest{Name: tapeName})
	if err != nil {
		log.Fatalf("could not get tape info: %v", err)
	}
	return r.GetInfo(), err
}

func (f *FrontApi) PutObject(isLocal bool, srcpath string) (dstpath string, e error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(30)*time.Second)
	defer cancel()
	if isLocal {
		rr, err := f.c.PutObject(ctx, &pb.PutObjectReqeust{Islocal: isLocal, Srcpath: srcpath})
		if err != nil {
			//to do add log
			return
		}
		return rr.GetDespath(), err
	} else {
		data, err := os.ReadFile(srcpath)
		size := int32(len(data))
		rr, err := f.c.PutObject(ctx, &pb.PutObjectReqeust{Islocal: isLocal, Srcpath: srcpath, Size: size, Binary: data})
		if err != nil {
			//to do add log
			return
		}
		return rr.GetDespath(), err
	}
}

func (f *FrontApi) PutLocalObject(srcpath string) (dstpath string, e error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(30)*time.Second)
	defer cancel()
	isLocal := true
	rr, err := f.c.PutObject(ctx, &pb.PutObjectReqeust{Islocal: isLocal, Srcpath: srcpath})
	if err != nil {
		//to do add log
	}
	return rr.GetDespath(), err
}

func (f *FrontApi) GetPoolsInfo() ([]*Pool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(30)*time.Second)
	defer cancel()
	reply, err := f.c.GetPoolsInfo(ctx, &pb.Empty{})
	if err != nil {
		return []*Pool{}, err
	} else {
		poolInfos := []*Pool{}
		for _, pool := range reply.Pools {
			poolInfos = append(poolInfos, &Pool{Poolname: pool.PoolName, Total: pool.Total, Free: pool.Free, Unref: pool.Unref, Numtapes: pool.Numtapes})
		}
		return poolInfos, err
	}

}

func (f *FrontApi) GetPoolInfo(name string) (Pool, bool) {
	pools, err := f.GetPoolsInfo()
	if err != nil {
		return Pool{}, false
	} else {
		for _, pool := range pools {
			if name == pool.Poolname {
				return *pool, true
			}
		}
		return Pool{}, false
	}
}

func (f *FrontApi) Migrate(file string, poolName string) (success bool, e error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(6000)*time.Second)
	defer cancel()
	status, err := f.c.Migrate(ctx, &pb.MigrateRequest{PoolName: poolName, Files: []string{file}})
	return status.Success, err
}

func (f *FrontApi) Recall(file string, resident bool) (success bool, e error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(3000)*time.Second)
	defer cancel()
	status, err := f.c.Recall(ctx, &pb.RecallRequest{Resident: resident, Files: []string{file}})
	return status.Success, err
}

func (f *FrontApi) Retrieve() error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(3000)*time.Second)
	defer cancel()
	_, err := f.c.Retrieve(ctx, &pb.Empty{})
	return err
}

func (f *FrontApi) MigrateAsync(file string, poolName string) (reqNumber uint64, e error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(30)*time.Second)
	defer cancel()
	status, err := f.c.MigrateAsync(ctx, &pb.MigrateRequest{PoolName: poolName, Files: []string{file}})
	return uint64(status.ReqNumber), err
}

func (f *FrontApi) RecallAsync(file string, resident bool) (reqNumber uint64, e error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(30)*time.Second)
	defer cancel()
	status, err := f.c.RecallAsync(ctx, &pb.RecallRequest{Resident: resident, Files: []string{file}})
	return uint64(status.ReqNumber), err
}

func (f *FrontApi) GetAsyncStatus(reqNumber uint64) (AsyncStatus, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(30)*time.Second)
	defer cancel()
	status, err := f.c.GetAsyncStatus(ctx, &pb.AsyncStatusRequest{ReqNumber: reqNumber})
	if err != nil {
		log.Fatalf("could not get async status: %v", err)
		return AsyncStatus{}, err
	}
	return AsyncStatus{ReqNumber: reqNumber, Success: status.Success, Resident: status.Resident,
		Transferred: status.Resident, Premigrated: status.Premigrated, Migrated: status.Migrated, Failed: status.Failed, Done: status.Done}, err
}

func (f *FrontApi) GetFileInfo(fileName string) (FileInfo, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(30)*time.Second)
	defer cancel()
	fileInfo, err := f.c.GetFileInfo(ctx, &pb.FileInfoRequest{FileName: fileName})
	if err != nil {
		log.Fatalf("Get file info failed with err: %v", err)
		return FileInfo{}, err
	}
	return FileInfo{MigState: fileInfo.MigState, FileName: fileInfo.FileName, Size: fileInfo.Size, Blocks: fileInfo.Blocks,
		Fsidh: fileInfo.Fsidh, Fsidl: fileInfo.Fsidl, Igen: fileInfo.Igen, Inum: fileInfo.Inum, TapeId: fileInfo.TapeId,
		StartBlock: fileInfo.StartBlock}, err
}

func (f *FrontApi) GetRawFileInfo(fileName string) (fileInfo *pb.FileInfo, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(30)*time.Second)
	defer cancel()
	fileInfo, err = f.c.GetFileInfo(ctx, &pb.FileInfoRequest{FileName: fileName})
	return fileInfo, err
}
