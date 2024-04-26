# Generated by the gRPC Python protocol compiler plugin. DO NOT EDIT!
"""Client and server classes corresponding to protobuf-defined services."""
import grpc

from google.protobuf import empty_pb2 as google_dot_protobuf_dot_empty__pb2
import cache_coordinator_pb2 as proto_dot_cache__coordinator__pb2


class CacheCoordinatorServiceStub(object):
    """
    Command to create stub files:
    python -m grpc_tools.protoc --proto_path=. ./proto/cache_coordinator.proto --python_out=. --grpc_python_out=.
    rpc RegisterJob(JobInfo) returns (RegisterJobResponse);
    rpc SendMetrics(MetricsRequest) returns (google.protobuf.Empty);
    """

    def __init__(self, channel):
        """Constructor.

        Args:
            channel: A grpc.Channel.
        """
        self.GetBatchStatus = channel.unary_unary(
                '/CacheCoordinatorService/GetBatchStatus',
                request_serializer=proto_dot_cache__coordinator__pb2.GetBatchStatusRequest.SerializeToString,
                response_deserializer=proto_dot_cache__coordinator__pb2.GetBatchStatusResponse.FromString,
                )
        self.RegisterDataset = channel.unary_unary(
                '/CacheCoordinatorService/RegisterDataset',
                request_serializer=proto_dot_cache__coordinator__pb2.RegisterDatasetInfo.SerializeToString,
                response_deserializer=proto_dot_cache__coordinator__pb2.RegisterDatasetResponse.FromString,
                )
        self.RegisterJob = channel.unary_unary(
                '/CacheCoordinatorService/RegisterJob',
                request_serializer=proto_dot_cache__coordinator__pb2.RegisterJobInfo.SerializeToString,
                response_deserializer=proto_dot_cache__coordinator__pb2.RegisterJobResponse.FromString,
                )
        self.ShareBatchAccessPattern = channel.unary_unary(
                '/CacheCoordinatorService/ShareBatchAccessPattern',
                request_serializer=proto_dot_cache__coordinator__pb2.BatchAccessPatternList.SerializeToString,
                response_deserializer=google_dot_protobuf_dot_empty__pb2.Empty.FromString,
                )
        self.ShareJobMetrics = channel.unary_unary(
                '/CacheCoordinatorService/ShareJobMetrics',
                request_serializer=proto_dot_cache__coordinator__pb2.JobMetricsInfo.SerializeToString,
                response_deserializer=google_dot_protobuf_dot_empty__pb2.Empty.FromString,
                )
        self.PingServer = channel.unary_unary(
                '/CacheCoordinatorService/PingServer',
                request_serializer=proto_dot_cache__coordinator__pb2.GetPingServerRequest.SerializeToString,
                response_deserializer=proto_dot_cache__coordinator__pb2.GetPingServerResponse.FromString,
                )


class CacheCoordinatorServiceServicer(object):
    """
    Command to create stub files:
    python -m grpc_tools.protoc --proto_path=. ./proto/cache_coordinator.proto --python_out=. --grpc_python_out=.
    rpc RegisterJob(JobInfo) returns (RegisterJobResponse);
    rpc SendMetrics(MetricsRequest) returns (google.protobuf.Empty);
    """

    def GetBatchStatus(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def RegisterDataset(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def RegisterJob(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def ShareBatchAccessPattern(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def ShareJobMetrics(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def PingServer(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')


def add_CacheCoordinatorServiceServicer_to_server(servicer, server):
    rpc_method_handlers = {
            'GetBatchStatus': grpc.unary_unary_rpc_method_handler(
                    servicer.GetBatchStatus,
                    request_deserializer=proto_dot_cache__coordinator__pb2.GetBatchStatusRequest.FromString,
                    response_serializer=proto_dot_cache__coordinator__pb2.GetBatchStatusResponse.SerializeToString,
            ),
            'RegisterDataset': grpc.unary_unary_rpc_method_handler(
                    servicer.RegisterDataset,
                    request_deserializer=proto_dot_cache__coordinator__pb2.RegisterDatasetInfo.FromString,
                    response_serializer=proto_dot_cache__coordinator__pb2.RegisterDatasetResponse.SerializeToString,
            ),
            'RegisterJob': grpc.unary_unary_rpc_method_handler(
                    servicer.RegisterJob,
                    request_deserializer=proto_dot_cache__coordinator__pb2.RegisterJobInfo.FromString,
                    response_serializer=proto_dot_cache__coordinator__pb2.RegisterJobResponse.SerializeToString,
            ),
            'ShareBatchAccessPattern': grpc.unary_unary_rpc_method_handler(
                    servicer.ShareBatchAccessPattern,
                    request_deserializer=proto_dot_cache__coordinator__pb2.BatchAccessPatternList.FromString,
                    response_serializer=google_dot_protobuf_dot_empty__pb2.Empty.SerializeToString,
            ),
            'ShareJobMetrics': grpc.unary_unary_rpc_method_handler(
                    servicer.ShareJobMetrics,
                    request_deserializer=proto_dot_cache__coordinator__pb2.JobMetricsInfo.FromString,
                    response_serializer=google_dot_protobuf_dot_empty__pb2.Empty.SerializeToString,
            ),
            'PingServer': grpc.unary_unary_rpc_method_handler(
                    servicer.PingServer,
                    request_deserializer=proto_dot_cache__coordinator__pb2.GetPingServerRequest.FromString,
                    response_serializer=proto_dot_cache__coordinator__pb2.GetPingServerResponse.SerializeToString,
            ),
    }
    generic_handler = grpc.method_handlers_generic_handler(
            'CacheCoordinatorService', rpc_method_handlers)
    server.add_generic_rpc_handlers((generic_handler,))


 # This class is part of an EXPERIMENTAL API.
class CacheCoordinatorService(object):
    """
    Command to create stub files:
    python -m grpc_tools.protoc --proto_path=. ./proto/cache_coordinator.proto --python_out=. --grpc_python_out=.
    rpc RegisterJob(JobInfo) returns (RegisterJobResponse);
    rpc SendMetrics(MetricsRequest) returns (google.protobuf.Empty);
    """

    @staticmethod
    def GetBatchStatus(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/CacheCoordinatorService/GetBatchStatus',
            proto_dot_cache__coordinator__pb2.GetBatchStatusRequest.SerializeToString,
            proto_dot_cache__coordinator__pb2.GetBatchStatusResponse.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def RegisterDataset(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/CacheCoordinatorService/RegisterDataset',
            proto_dot_cache__coordinator__pb2.RegisterDatasetInfo.SerializeToString,
            proto_dot_cache__coordinator__pb2.RegisterDatasetResponse.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def RegisterJob(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/CacheCoordinatorService/RegisterJob',
            proto_dot_cache__coordinator__pb2.RegisterJobInfo.SerializeToString,
            proto_dot_cache__coordinator__pb2.RegisterJobResponse.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def ShareBatchAccessPattern(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/CacheCoordinatorService/ShareBatchAccessPattern',
            proto_dot_cache__coordinator__pb2.BatchAccessPatternList.SerializeToString,
            google_dot_protobuf_dot_empty__pb2.Empty.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def ShareJobMetrics(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/CacheCoordinatorService/ShareJobMetrics',
            proto_dot_cache__coordinator__pb2.JobMetricsInfo.SerializeToString,
            google_dot_protobuf_dot_empty__pb2.Empty.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def PingServer(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/CacheCoordinatorService/PingServer',
            proto_dot_cache__coordinator__pb2.GetPingServerRequest.SerializeToString,
            proto_dot_cache__coordinator__pb2.GetPingServerResponse.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)