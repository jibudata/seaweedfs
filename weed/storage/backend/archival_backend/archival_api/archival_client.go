package archival_client

import (
	"context"
	"log"
	"os"
	"time"

	"google.golang.org/grpc"
	// pb "test/arch"
	pb "github.com/chrislusf/seaweedfs/weed/pb/archival"
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
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(30)*time.Second)
	defer cancel()
	status, err := f.c.Migrate(ctx, &pb.MigrateRequest{PoolName: poolName, Files: []string{file}})
	return status.Success, err
}

func (f *FrontApi) Recall(file string, resident bool) (success bool, e error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(30)*time.Second)
	defer cancel()
	status, err := f.c.Recall(ctx, &pb.RecallRequest{Resident: resident, Files: []string{file}})
	return status.Success, err
}

func (f *FrontApi) Retrieve() error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(30)*time.Second)
	defer cancel()
	_, err := f.c.Retrieve(ctx, &pb.Empty{})
	return err
}
