// Code generated by protoc-gen-go-grpc. DO NOT EDIT.

package protobuf

import (
	context "context"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
// Requires gRPC-Go v1.32.0 or later.
const _ = grpc.SupportPackageIsVersion7

// ArchivalerClient is the client API for Archivaler service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type ArchivalerClient interface {
	GetTapeInfo(ctx context.Context, in *TapeInfoRequest, opts ...grpc.CallOption) (*TapeInfoReply, error)
	PutObject(ctx context.Context, in *PutObjectReqeust, opts ...grpc.CallOption) (*PutObjectReply, error)
}

type archivalerClient struct {
	cc grpc.ClientConnInterface
}

func NewArchivalerClient(cc grpc.ClientConnInterface) ArchivalerClient {
	return &archivalerClient{cc}
}

func (c *archivalerClient) GetTapeInfo(ctx context.Context, in *TapeInfoRequest, opts ...grpc.CallOption) (*TapeInfoReply, error) {
	out := new(TapeInfoReply)
	err := c.cc.Invoke(ctx, "/protobuf.Archivaler/GetTapeInfo", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *archivalerClient) PutObject(ctx context.Context, in *PutObjectReqeust, opts ...grpc.CallOption) (*PutObjectReply, error) {
	out := new(PutObjectReply)
	err := c.cc.Invoke(ctx, "/protobuf.Archivaler/PutObject", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// ArchivalerServer is the server API for Archivaler service.
// All implementations must embed UnimplementedArchivalerServer
// for forward compatibility
type ArchivalerServer interface {
	GetTapeInfo(context.Context, *TapeInfoRequest) (*TapeInfoReply, error)
	PutObject(context.Context, *PutObjectReqeust) (*PutObjectReply, error)
	mustEmbedUnimplementedArchivalerServer()
}

// UnimplementedArchivalerServer must be embedded to have forward compatible implementations.
type UnimplementedArchivalerServer struct {
}

func (UnimplementedArchivalerServer) GetTapeInfo(context.Context, *TapeInfoRequest) (*TapeInfoReply, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetTapeInfo not implemented")
}
func (UnimplementedArchivalerServer) PutObject(context.Context, *PutObjectReqeust) (*PutObjectReply, error) {
	return nil, status.Errorf(codes.Unimplemented, "method PutObject not implemented")
}
func (UnimplementedArchivalerServer) mustEmbedUnimplementedArchivalerServer() {}

// UnsafeArchivalerServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to ArchivalerServer will
// result in compilation errors.
type UnsafeArchivalerServer interface {
	mustEmbedUnimplementedArchivalerServer()
}

func RegisterArchivalerServer(s grpc.ServiceRegistrar, srv ArchivalerServer) {
	s.RegisterService(&Archivaler_ServiceDesc, srv)
}

func _Archivaler_GetTapeInfo_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(TapeInfoRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ArchivalerServer).GetTapeInfo(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/protobuf.Archivaler/GetTapeInfo",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ArchivalerServer).GetTapeInfo(ctx, req.(*TapeInfoRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Archivaler_PutObject_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(PutObjectReqeust)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ArchivalerServer).PutObject(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/protobuf.Archivaler/PutObject",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ArchivalerServer).PutObject(ctx, req.(*PutObjectReqeust))
	}
	return interceptor(ctx, in, info, handler)
}

// Archivaler_ServiceDesc is the grpc.ServiceDesc for Archivaler service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var Archivaler_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "protobuf.Archivaler",
	HandlerType: (*ArchivalerServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "GetTapeInfo",
			Handler:    _Archivaler_GetTapeInfo_Handler,
		},
		{
			MethodName: "PutObject",
			Handler:    _Archivaler_PutObject_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "arch.proto",
}
