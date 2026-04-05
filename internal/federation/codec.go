package federation

import (
	"encoding/json"

	"google.golang.org/grpc"
	"google.golang.org/grpc/encoding"
)

func init() {
	// Register a JSON codec for the federation stream. Clients and servers that
	// use ForceCodecOption() or gRPC content-type application/grpc+json will
	// serialize federation.Envelope values as JSON instead of protobuf.
	encoding.RegisterCodec(federationJSONCodec{})
}

// federationJSONCodec implements encoding.Codec using standard JSON marshaling.
// Used by both FederationServer and FederationClient to encode/decode Envelope.
type federationJSONCodec struct{}

func (federationJSONCodec) Marshal(v any) ([]byte, error)      { return json.Marshal(v) }
func (federationJSONCodec) Unmarshal(data []byte, v any) error { return json.Unmarshal(data, v) }
func (federationJSONCodec) Name() string                        { return "json" }

// ForceCodecOption returns a gRPC CallOption that forces the federation JSON
// codec for a stream. Pass this to conn.NewStream when opening a federation stream.
func ForceCodecOption() grpc.CallOption {
	return grpc.ForceCodec(federationJSONCodec{})
}
