// Code generated by protoc-gen-go-grpc. DO NOT EDIT.
// versions:
// - protoc-gen-go-grpc v1.2.0
// - protoc             v4.25.3
// source: deephaven_core/proto/session.proto

package session

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

// SessionServiceClient is the client API for SessionService service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type SessionServiceClient interface {
	// Deprecated: Do not use.
	//
	// Handshake between client and server to create a new session. The response includes a metadata header name and the
	// token to send on every subsequent request. The auth mechanisms here are unary to best support grpc-web.
	//
	// Deprecated: Please use Flight's Handshake or http authorization headers instead.
	NewSession(ctx context.Context, in *HandshakeRequest, opts ...grpc.CallOption) (*HandshakeResponse, error)
	// Deprecated: Do not use.
	//
	// Keep-alive a given token to ensure that a session is not cleaned prematurely. The response may include an updated
	// token that should replace the existing token for subsequent requests.
	//
	// Deprecated: Please use Flight's Handshake with an empty payload.
	RefreshSessionToken(ctx context.Context, in *HandshakeRequest, opts ...grpc.CallOption) (*HandshakeResponse, error)
	//
	// Proactively close an open session. Sessions will automatically close on timeout. When a session is closed, all
	// unreleased exports will be automatically released.
	CloseSession(ctx context.Context, in *HandshakeRequest, opts ...grpc.CallOption) (*CloseSessionResponse, error)
	//
	// Attempts to release an export by its ticket. Returns true if an existing export was found. It is the client's
	// responsibility to release all resources they no longer want the server to hold on to. Proactively cancels work; do
	// not release a ticket that is needed by dependent work that has not yet finished
	// (i.e. the dependencies that are staying around should first be in EXPORTED state).
	Release(ctx context.Context, in *ReleaseRequest, opts ...grpc.CallOption) (*ReleaseResponse, error)
	//
	// Makes a copy from a source ticket to a client managed result ticket. The source ticket does not need to be
	// a client managed ticket.
	ExportFromTicket(ctx context.Context, in *ExportRequest, opts ...grpc.CallOption) (*ExportResponse, error)
	//
	// Makes a copy from a source ticket and publishes to a result ticket. Neither the source ticket, nor the destination
	// ticket, need to be a client managed ticket.
	PublishFromTicket(ctx context.Context, in *PublishRequest, opts ...grpc.CallOption) (*PublishResponse, error)
	//
	// Establish a stream to manage all session exports, including those lost due to partially complete rpc calls.
	//
	// New streams will flush notifications for all un-released exports, prior to seeing any new or updated exports
	// for all live exports. After the refresh of existing state, subscribers will receive notifications of new and
	// updated exports. An export id of zero will be sent to indicate all pre-existing exports have been sent.
	ExportNotifications(ctx context.Context, in *ExportNotificationRequest, opts ...grpc.CallOption) (SessionService_ExportNotificationsClient, error)
	//
	// Receive a best-effort message on-exit indicating why this server is exiting. Reception of this message cannot be
	// guaranteed.
	TerminationNotification(ctx context.Context, in *TerminationNotificationRequest, opts ...grpc.CallOption) (*TerminationNotificationResponse, error)
}

type sessionServiceClient struct {
	cc grpc.ClientConnInterface
}

func NewSessionServiceClient(cc grpc.ClientConnInterface) SessionServiceClient {
	return &sessionServiceClient{cc}
}

// Deprecated: Do not use.
func (c *sessionServiceClient) NewSession(ctx context.Context, in *HandshakeRequest, opts ...grpc.CallOption) (*HandshakeResponse, error) {
	out := new(HandshakeResponse)
	err := c.cc.Invoke(ctx, "/io.deephaven.proto.backplane.grpc.SessionService/NewSession", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// Deprecated: Do not use.
func (c *sessionServiceClient) RefreshSessionToken(ctx context.Context, in *HandshakeRequest, opts ...grpc.CallOption) (*HandshakeResponse, error) {
	out := new(HandshakeResponse)
	err := c.cc.Invoke(ctx, "/io.deephaven.proto.backplane.grpc.SessionService/RefreshSessionToken", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *sessionServiceClient) CloseSession(ctx context.Context, in *HandshakeRequest, opts ...grpc.CallOption) (*CloseSessionResponse, error) {
	out := new(CloseSessionResponse)
	err := c.cc.Invoke(ctx, "/io.deephaven.proto.backplane.grpc.SessionService/CloseSession", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *sessionServiceClient) Release(ctx context.Context, in *ReleaseRequest, opts ...grpc.CallOption) (*ReleaseResponse, error) {
	out := new(ReleaseResponse)
	err := c.cc.Invoke(ctx, "/io.deephaven.proto.backplane.grpc.SessionService/Release", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *sessionServiceClient) ExportFromTicket(ctx context.Context, in *ExportRequest, opts ...grpc.CallOption) (*ExportResponse, error) {
	out := new(ExportResponse)
	err := c.cc.Invoke(ctx, "/io.deephaven.proto.backplane.grpc.SessionService/ExportFromTicket", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *sessionServiceClient) PublishFromTicket(ctx context.Context, in *PublishRequest, opts ...grpc.CallOption) (*PublishResponse, error) {
	out := new(PublishResponse)
	err := c.cc.Invoke(ctx, "/io.deephaven.proto.backplane.grpc.SessionService/PublishFromTicket", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *sessionServiceClient) ExportNotifications(ctx context.Context, in *ExportNotificationRequest, opts ...grpc.CallOption) (SessionService_ExportNotificationsClient, error) {
	stream, err := c.cc.NewStream(ctx, &SessionService_ServiceDesc.Streams[0], "/io.deephaven.proto.backplane.grpc.SessionService/ExportNotifications", opts...)
	if err != nil {
		return nil, err
	}
	x := &sessionServiceExportNotificationsClient{stream}
	if err := x.ClientStream.SendMsg(in); err != nil {
		return nil, err
	}
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	return x, nil
}

type SessionService_ExportNotificationsClient interface {
	Recv() (*ExportNotification, error)
	grpc.ClientStream
}

type sessionServiceExportNotificationsClient struct {
	grpc.ClientStream
}

func (x *sessionServiceExportNotificationsClient) Recv() (*ExportNotification, error) {
	m := new(ExportNotification)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

func (c *sessionServiceClient) TerminationNotification(ctx context.Context, in *TerminationNotificationRequest, opts ...grpc.CallOption) (*TerminationNotificationResponse, error) {
	out := new(TerminationNotificationResponse)
	err := c.cc.Invoke(ctx, "/io.deephaven.proto.backplane.grpc.SessionService/TerminationNotification", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// SessionServiceServer is the server API for SessionService service.
// All implementations must embed UnimplementedSessionServiceServer
// for forward compatibility
type SessionServiceServer interface {
	// Deprecated: Do not use.
	//
	// Handshake between client and server to create a new session. The response includes a metadata header name and the
	// token to send on every subsequent request. The auth mechanisms here are unary to best support grpc-web.
	//
	// Deprecated: Please use Flight's Handshake or http authorization headers instead.
	NewSession(context.Context, *HandshakeRequest) (*HandshakeResponse, error)
	// Deprecated: Do not use.
	//
	// Keep-alive a given token to ensure that a session is not cleaned prematurely. The response may include an updated
	// token that should replace the existing token for subsequent requests.
	//
	// Deprecated: Please use Flight's Handshake with an empty payload.
	RefreshSessionToken(context.Context, *HandshakeRequest) (*HandshakeResponse, error)
	//
	// Proactively close an open session. Sessions will automatically close on timeout. When a session is closed, all
	// unreleased exports will be automatically released.
	CloseSession(context.Context, *HandshakeRequest) (*CloseSessionResponse, error)
	//
	// Attempts to release an export by its ticket. Returns true if an existing export was found. It is the client's
	// responsibility to release all resources they no longer want the server to hold on to. Proactively cancels work; do
	// not release a ticket that is needed by dependent work that has not yet finished
	// (i.e. the dependencies that are staying around should first be in EXPORTED state).
	Release(context.Context, *ReleaseRequest) (*ReleaseResponse, error)
	//
	// Makes a copy from a source ticket to a client managed result ticket. The source ticket does not need to be
	// a client managed ticket.
	ExportFromTicket(context.Context, *ExportRequest) (*ExportResponse, error)
	//
	// Makes a copy from a source ticket and publishes to a result ticket. Neither the source ticket, nor the destination
	// ticket, need to be a client managed ticket.
	PublishFromTicket(context.Context, *PublishRequest) (*PublishResponse, error)
	//
	// Establish a stream to manage all session exports, including those lost due to partially complete rpc calls.
	//
	// New streams will flush notifications for all un-released exports, prior to seeing any new or updated exports
	// for all live exports. After the refresh of existing state, subscribers will receive notifications of new and
	// updated exports. An export id of zero will be sent to indicate all pre-existing exports have been sent.
	ExportNotifications(*ExportNotificationRequest, SessionService_ExportNotificationsServer) error
	//
	// Receive a best-effort message on-exit indicating why this server is exiting. Reception of this message cannot be
	// guaranteed.
	TerminationNotification(context.Context, *TerminationNotificationRequest) (*TerminationNotificationResponse, error)
	mustEmbedUnimplementedSessionServiceServer()
}

// UnimplementedSessionServiceServer must be embedded to have forward compatible implementations.
type UnimplementedSessionServiceServer struct {
}

func (UnimplementedSessionServiceServer) NewSession(context.Context, *HandshakeRequest) (*HandshakeResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method NewSession not implemented")
}
func (UnimplementedSessionServiceServer) RefreshSessionToken(context.Context, *HandshakeRequest) (*HandshakeResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method RefreshSessionToken not implemented")
}
func (UnimplementedSessionServiceServer) CloseSession(context.Context, *HandshakeRequest) (*CloseSessionResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method CloseSession not implemented")
}
func (UnimplementedSessionServiceServer) Release(context.Context, *ReleaseRequest) (*ReleaseResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Release not implemented")
}
func (UnimplementedSessionServiceServer) ExportFromTicket(context.Context, *ExportRequest) (*ExportResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ExportFromTicket not implemented")
}
func (UnimplementedSessionServiceServer) PublishFromTicket(context.Context, *PublishRequest) (*PublishResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method PublishFromTicket not implemented")
}
func (UnimplementedSessionServiceServer) ExportNotifications(*ExportNotificationRequest, SessionService_ExportNotificationsServer) error {
	return status.Errorf(codes.Unimplemented, "method ExportNotifications not implemented")
}
func (UnimplementedSessionServiceServer) TerminationNotification(context.Context, *TerminationNotificationRequest) (*TerminationNotificationResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method TerminationNotification not implemented")
}
func (UnimplementedSessionServiceServer) mustEmbedUnimplementedSessionServiceServer() {}

// UnsafeSessionServiceServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to SessionServiceServer will
// result in compilation errors.
type UnsafeSessionServiceServer interface {
	mustEmbedUnimplementedSessionServiceServer()
}

func RegisterSessionServiceServer(s grpc.ServiceRegistrar, srv SessionServiceServer) {
	s.RegisterService(&SessionService_ServiceDesc, srv)
}

func _SessionService_NewSession_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(HandshakeRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(SessionServiceServer).NewSession(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/io.deephaven.proto.backplane.grpc.SessionService/NewSession",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(SessionServiceServer).NewSession(ctx, req.(*HandshakeRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _SessionService_RefreshSessionToken_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(HandshakeRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(SessionServiceServer).RefreshSessionToken(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/io.deephaven.proto.backplane.grpc.SessionService/RefreshSessionToken",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(SessionServiceServer).RefreshSessionToken(ctx, req.(*HandshakeRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _SessionService_CloseSession_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(HandshakeRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(SessionServiceServer).CloseSession(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/io.deephaven.proto.backplane.grpc.SessionService/CloseSession",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(SessionServiceServer).CloseSession(ctx, req.(*HandshakeRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _SessionService_Release_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ReleaseRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(SessionServiceServer).Release(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/io.deephaven.proto.backplane.grpc.SessionService/Release",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(SessionServiceServer).Release(ctx, req.(*ReleaseRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _SessionService_ExportFromTicket_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ExportRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(SessionServiceServer).ExportFromTicket(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/io.deephaven.proto.backplane.grpc.SessionService/ExportFromTicket",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(SessionServiceServer).ExportFromTicket(ctx, req.(*ExportRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _SessionService_PublishFromTicket_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(PublishRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(SessionServiceServer).PublishFromTicket(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/io.deephaven.proto.backplane.grpc.SessionService/PublishFromTicket",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(SessionServiceServer).PublishFromTicket(ctx, req.(*PublishRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _SessionService_ExportNotifications_Handler(srv interface{}, stream grpc.ServerStream) error {
	m := new(ExportNotificationRequest)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(SessionServiceServer).ExportNotifications(m, &sessionServiceExportNotificationsServer{stream})
}

type SessionService_ExportNotificationsServer interface {
	Send(*ExportNotification) error
	grpc.ServerStream
}

type sessionServiceExportNotificationsServer struct {
	grpc.ServerStream
}

func (x *sessionServiceExportNotificationsServer) Send(m *ExportNotification) error {
	return x.ServerStream.SendMsg(m)
}

func _SessionService_TerminationNotification_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(TerminationNotificationRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(SessionServiceServer).TerminationNotification(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/io.deephaven.proto.backplane.grpc.SessionService/TerminationNotification",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(SessionServiceServer).TerminationNotification(ctx, req.(*TerminationNotificationRequest))
	}
	return interceptor(ctx, in, info, handler)
}

// SessionService_ServiceDesc is the grpc.ServiceDesc for SessionService service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var SessionService_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "io.deephaven.proto.backplane.grpc.SessionService",
	HandlerType: (*SessionServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "NewSession",
			Handler:    _SessionService_NewSession_Handler,
		},
		{
			MethodName: "RefreshSessionToken",
			Handler:    _SessionService_RefreshSessionToken_Handler,
		},
		{
			MethodName: "CloseSession",
			Handler:    _SessionService_CloseSession_Handler,
		},
		{
			MethodName: "Release",
			Handler:    _SessionService_Release_Handler,
		},
		{
			MethodName: "ExportFromTicket",
			Handler:    _SessionService_ExportFromTicket_Handler,
		},
		{
			MethodName: "PublishFromTicket",
			Handler:    _SessionService_PublishFromTicket_Handler,
		},
		{
			MethodName: "TerminationNotification",
			Handler:    _SessionService_TerminationNotification_Handler,
		},
	},
	Streams: []grpc.StreamDesc{
		{
			StreamName:    "ExportNotifications",
			Handler:       _SessionService_ExportNotifications_Handler,
			ServerStreams: true,
		},
	},
	Metadata: "deephaven_core/proto/session.proto",
}
