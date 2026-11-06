"""gRPC client stub for gcsfs.v1.FileSystemService."""

from gcsfs._proto import filesystem_pb2 as _pb2


class FileSystemServiceStub:
    """Async/sync gRPC stub for gcsfs.v1.FileSystemService."""

    def __init__(self, channel):
        self.Info = channel.unary_unary(
            "/gcsfs.v1.FileSystemService/Info",
            request_serializer=_pb2.InfoRequest.SerializeToString,
            response_deserializer=_pb2.InfoResponse.FromString,
        )
        self.List = channel.unary_unary(
            "/gcsfs.v1.FileSystemService/List",
            request_serializer=_pb2.ListRequest.SerializeToString,
            response_deserializer=_pb2.ListResponse.FromString,
        )
        self.Read = channel.unary_stream(
            "/gcsfs.v1.FileSystemService/Read",
            request_serializer=_pb2.ReadRequest.SerializeToString,
            response_deserializer=_pb2.ReadResponse.FromString,
        )
        self.Write = channel.stream_unary(
            "/gcsfs.v1.FileSystemService/Write",
            request_serializer=_pb2.WriteRequest.SerializeToString,
            response_deserializer=_pb2.WriteResponse.FromString,
        )

