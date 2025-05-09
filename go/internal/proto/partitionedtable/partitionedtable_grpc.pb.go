// Code generated by protoc-gen-go-grpc. DO NOT EDIT.
// versions:
// - protoc-gen-go-grpc v1.2.0
// - protoc             v4.25.3
// source: deephaven_core/proto/partitionedtable.proto

package partitionedtable

import (
	context "context"
	table "github.com/deephaven/deephaven-core/go/internal/proto/table"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
// Requires gRPC-Go v1.32.0 or later.
const _ = grpc.SupportPackageIsVersion7

// PartitionedTableServiceClient is the client API for PartitionedTableService service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type PartitionedTableServiceClient interface {
	//
	// Transforms a table into a partitioned table, consisting of many separate tables, each individually
	// addressable. The result will be a FetchObjectResponse populated with a PartitionedTable.
	PartitionBy(ctx context.Context, in *PartitionByRequest, opts ...grpc.CallOption) (*PartitionByResponse, error)
	//
	// Given a partitioned table, returns a table with the contents of all of the constituent tables.
	Merge(ctx context.Context, in *MergeRequest, opts ...grpc.CallOption) (*table.ExportedTableCreationResponse, error)
	//
	// Given a partitioned table and a row described by another table's contents, returns a table
	// that matched that row, if any. If none is present, NOT_FOUND will be sent in response. If
	// more than one is present, FAILED_PRECONDITION will be sent in response.
	//
	// If the provided key table has any number of rows other than one, INVALID_ARGUMENT will be
	// sent in response.
	//
	// The simplest way to generally use this is to subscribe to the key columns of the underlying
	// table of a given PartitionedTable, then use /FlightService/DoPut to create a table with the
	// desired keys, and pass that ticket to this service. After that request is sent (note that it
	// is not required to wait for it to complete), that new table ticket can be used to make this
	// GetTable request.
	GetTable(ctx context.Context, in *GetTableRequest, opts ...grpc.CallOption) (*table.ExportedTableCreationResponse, error)
}

type partitionedTableServiceClient struct {
	cc grpc.ClientConnInterface
}

func NewPartitionedTableServiceClient(cc grpc.ClientConnInterface) PartitionedTableServiceClient {
	return &partitionedTableServiceClient{cc}
}

func (c *partitionedTableServiceClient) PartitionBy(ctx context.Context, in *PartitionByRequest, opts ...grpc.CallOption) (*PartitionByResponse, error) {
	out := new(PartitionByResponse)
	err := c.cc.Invoke(ctx, "/io.deephaven.proto.backplane.grpc.PartitionedTableService/PartitionBy", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *partitionedTableServiceClient) Merge(ctx context.Context, in *MergeRequest, opts ...grpc.CallOption) (*table.ExportedTableCreationResponse, error) {
	out := new(table.ExportedTableCreationResponse)
	err := c.cc.Invoke(ctx, "/io.deephaven.proto.backplane.grpc.PartitionedTableService/Merge", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *partitionedTableServiceClient) GetTable(ctx context.Context, in *GetTableRequest, opts ...grpc.CallOption) (*table.ExportedTableCreationResponse, error) {
	out := new(table.ExportedTableCreationResponse)
	err := c.cc.Invoke(ctx, "/io.deephaven.proto.backplane.grpc.PartitionedTableService/GetTable", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// PartitionedTableServiceServer is the server API for PartitionedTableService service.
// All implementations must embed UnimplementedPartitionedTableServiceServer
// for forward compatibility
type PartitionedTableServiceServer interface {
	//
	// Transforms a table into a partitioned table, consisting of many separate tables, each individually
	// addressable. The result will be a FetchObjectResponse populated with a PartitionedTable.
	PartitionBy(context.Context, *PartitionByRequest) (*PartitionByResponse, error)
	//
	// Given a partitioned table, returns a table with the contents of all of the constituent tables.
	Merge(context.Context, *MergeRequest) (*table.ExportedTableCreationResponse, error)
	//
	// Given a partitioned table and a row described by another table's contents, returns a table
	// that matched that row, if any. If none is present, NOT_FOUND will be sent in response. If
	// more than one is present, FAILED_PRECONDITION will be sent in response.
	//
	// If the provided key table has any number of rows other than one, INVALID_ARGUMENT will be
	// sent in response.
	//
	// The simplest way to generally use this is to subscribe to the key columns of the underlying
	// table of a given PartitionedTable, then use /FlightService/DoPut to create a table with the
	// desired keys, and pass that ticket to this service. After that request is sent (note that it
	// is not required to wait for it to complete), that new table ticket can be used to make this
	// GetTable request.
	GetTable(context.Context, *GetTableRequest) (*table.ExportedTableCreationResponse, error)
	mustEmbedUnimplementedPartitionedTableServiceServer()
}

// UnimplementedPartitionedTableServiceServer must be embedded to have forward compatible implementations.
type UnimplementedPartitionedTableServiceServer struct {
}

func (UnimplementedPartitionedTableServiceServer) PartitionBy(context.Context, *PartitionByRequest) (*PartitionByResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method PartitionBy not implemented")
}
func (UnimplementedPartitionedTableServiceServer) Merge(context.Context, *MergeRequest) (*table.ExportedTableCreationResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Merge not implemented")
}
func (UnimplementedPartitionedTableServiceServer) GetTable(context.Context, *GetTableRequest) (*table.ExportedTableCreationResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetTable not implemented")
}
func (UnimplementedPartitionedTableServiceServer) mustEmbedUnimplementedPartitionedTableServiceServer() {
}

// UnsafePartitionedTableServiceServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to PartitionedTableServiceServer will
// result in compilation errors.
type UnsafePartitionedTableServiceServer interface {
	mustEmbedUnimplementedPartitionedTableServiceServer()
}

func RegisterPartitionedTableServiceServer(s grpc.ServiceRegistrar, srv PartitionedTableServiceServer) {
	s.RegisterService(&PartitionedTableService_ServiceDesc, srv)
}

func _PartitionedTableService_PartitionBy_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(PartitionByRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(PartitionedTableServiceServer).PartitionBy(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/io.deephaven.proto.backplane.grpc.PartitionedTableService/PartitionBy",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(PartitionedTableServiceServer).PartitionBy(ctx, req.(*PartitionByRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _PartitionedTableService_Merge_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(MergeRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(PartitionedTableServiceServer).Merge(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/io.deephaven.proto.backplane.grpc.PartitionedTableService/Merge",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(PartitionedTableServiceServer).Merge(ctx, req.(*MergeRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _PartitionedTableService_GetTable_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(GetTableRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(PartitionedTableServiceServer).GetTable(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/io.deephaven.proto.backplane.grpc.PartitionedTableService/GetTable",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(PartitionedTableServiceServer).GetTable(ctx, req.(*GetTableRequest))
	}
	return interceptor(ctx, in, info, handler)
}

// PartitionedTableService_ServiceDesc is the grpc.ServiceDesc for PartitionedTableService service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var PartitionedTableService_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "io.deephaven.proto.backplane.grpc.PartitionedTableService",
	HandlerType: (*PartitionedTableServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "PartitionBy",
			Handler:    _PartitionedTableService_PartitionBy_Handler,
		},
		{
			MethodName: "Merge",
			Handler:    _PartitionedTableService_Merge_Handler,
		},
		{
			MethodName: "GetTable",
			Handler:    _PartitionedTableService_GetTable_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "deephaven_core/proto/partitionedtable.proto",
}
