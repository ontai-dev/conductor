package permissionservice

import (
	"context"
	"encoding/json"
	"net"

	"google.golang.org/grpc"
	"google.golang.org/grpc/encoding"
)

func init() {
	// Register a JSON codec so the service can be called with content-type
	// application/grpc+json. This supplements the default protobuf codec —
	// it does not replace it.
	encoding.RegisterCodec(jsonCodec{})
}

// jsonCodec implements encoding.Codec using standard JSON marshaling.
// Clients must send content-type: application/grpc+json to use this codec.
type jsonCodec struct{}

func (jsonCodec) Marshal(v any) ([]byte, error)      { return json.Marshal(v) }
func (jsonCodec) Unmarshal(data []byte, v any) error { return json.Unmarshal(data, v) }
func (jsonCodec) Name() string                        { return "json" }

// PermissionServiceServer is the interface that LocalService satisfies.
// It must be implemented by any type registered with the gRPC server.
//
// The method signatures and service name are identical to the guardian management
// cluster PermissionService so that callers can use the same client stub against
// either endpoint. guardian-schema.md §10.
type PermissionServiceServer interface {
	CheckPermission(ctx context.Context, req *CheckPermissionRequest) (*CheckPermissionResponse, error)
	ListPermissions(ctx context.Context, req *ListPermissionsRequest) (*ListPermissionsResponse, error)
	WhoCanDo(ctx context.Context, req *WhoCanDoRequest) (*WhoCanDoResponse, error)
	ExplainDecision(ctx context.Context, req *ExplainDecisionRequest) (*ExplainDecisionResponse, error)
}

// permissionServiceDesc is the gRPC service descriptor for the local
// PermissionService. Service name is identical to the guardian endpoint so
// callers transparently address either cluster's implementation.
// conductor-schema.md §10 step 6, guardian-schema.md §10.
var permissionServiceDesc = grpc.ServiceDesc{
	ServiceName: "ontai.security.v1alpha1.PermissionService",
	HandlerType: (*PermissionServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{MethodName: "CheckPermission", Handler: _checkPermissionHandler},
		{MethodName: "ListPermissions", Handler: _listPermissionsHandler},
		{MethodName: "WhoCanDo", Handler: _whoCanDoHandler},
		{MethodName: "ExplainDecision", Handler: _explainDecisionHandler},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "ontai/security/v1alpha1/permissionservice.proto",
}

func _checkPermissionHandler(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	in := new(CheckPermissionRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(PermissionServiceServer).CheckPermission(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: "/ontai.security.v1alpha1.PermissionService/CheckPermission"}
	handler := func(ctx context.Context, req any) (any, error) {
		return srv.(PermissionServiceServer).CheckPermission(ctx, req.(*CheckPermissionRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _listPermissionsHandler(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	in := new(ListPermissionsRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(PermissionServiceServer).ListPermissions(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: "/ontai.security.v1alpha1.PermissionService/ListPermissions"}
	handler := func(ctx context.Context, req any) (any, error) {
		return srv.(PermissionServiceServer).ListPermissions(ctx, req.(*ListPermissionsRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _whoCanDoHandler(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	in := new(WhoCanDoRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(PermissionServiceServer).WhoCanDo(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: "/ontai.security.v1alpha1.PermissionService/WhoCanDo"}
	handler := func(ctx context.Context, req any) (any, error) {
		return srv.(PermissionServiceServer).WhoCanDo(ctx, req.(*WhoCanDoRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _explainDecisionHandler(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	in := new(ExplainDecisionRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(PermissionServiceServer).ExplainDecision(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: "/ontai.security.v1alpha1.PermissionService/ExplainDecision"}
	handler := func(ctx context.Context, req any) (any, error) {
		return srv.(PermissionServiceServer).ExplainDecision(ctx, req.(*ExplainDecisionRequest))
	}
	return interceptor(ctx, in, info, handler)
}

// NewGRPCServer creates a new gRPC server with the local PermissionService registered.
func NewGRPCServer(svc *LocalService) *grpc.Server {
	s := grpc.NewServer()
	s.RegisterService(&permissionServiceDesc, svc)
	return s
}

// ListenAndServe starts the local PermissionService gRPC server on the given
// address. It blocks until ctx is cancelled, at which point it performs a
// graceful stop. Intended to be run in a goroutine.
// conductor-schema.md §10 step 6.
func ListenAndServe(ctx context.Context, addr string, svc *LocalService) error {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	srv := NewGRPCServer(svc)
	go func() {
		<-ctx.Done()
		srv.GracefulStop()
	}()
	return srv.Serve(lis)
}
