import grpc
from concurrent import futures
import superdl.proto.cache_coordinator_pb2 as cache_coordinator_pb2
import superdl.proto.cache_coordinator_pb2_grpc as cache_coordinator_pb2_grpc
import google.protobuf.empty_pb2
from logger_config import logger
from coordinator import SUPERCoordinator
import hydra
from omegaconf import DictConfig
from args import SUPERArgs
from batch import Batch
from typing import Dict, List
import time

class CacheCoordinatorService(cache_coordinator_pb2_grpc.CacheCoordinatorServiceServicer):
    def __init__(self, coordinator: SUPERCoordinator):
        self.coordinator = coordinator

    def Ping(self, request, context):
        message = request.message
        return cache_coordinator_pb2.PingResponse(message = 'pong')
    
    def RegisterJob(self, request, context):
        success, message = self.coordinator.create_new_job(request.job_id,request.data_dir)
        logger.info(message)
        return cache_coordinator_pb2.RegisterJobResponse(job_registered=success, message = message)
    
    def GetDatasetInfo(self, request, context):
        num_files, num_chunks,chunk_size =  self.coordinator.get_dataset_info(request.data_dir)
        return cache_coordinator_pb2.DatasetInfoResponse(num_files=num_files, num_chunks=num_chunks,chunk_size=chunk_size)
    
    def GetNextBatchToProcess(self, request, context):
        
        batches:List[Batch] = self.coordinator.next_batch_for_job(request.job_id,request.num_batches_requested)
        # Convert batches to gRPC message format
        batch_messages =[cache_coordinator_pb2.Batch(batch_id=batch.batch_id, 
                                                     indicies=batch.indicies, 
                                                     is_cached=batch.is_cached) for batch in batches]

        # logger.info(f"Sending next {len(batches)} batches to job'{request.job_id}'")

        # Create and return the response
        response = cache_coordinator_pb2.GetNextBatchResponse(
            job_id=request.job_id,
            batches=batch_messages
            )
        return response
    
    def JobEnded(self, request, context):
        self.coordinator.handle_job_ended(job_id=request.job_id)
        logger.info(f"Job'{request.job_id}' has ended.")
        return google.protobuf.empty_pb2.Empty()

       

@hydra.main(version_base=None, config_path="../conf", config_name="config")
def serve(config: DictConfig):
    try:
        logger.info("Starting SUPER Datloading Service")   
        super_args:SUPERArgs = SUPERArgs(
            workload_kind =config.workload.kind,
            s3_data_dir = config.workload.s3_data_dir,
            batch_creation_lambda = config.batch_creation_lambda,
            batch_size = config.workload.batch_size,
            drop_last=config.workload.drop_last,
            simulate_mode = config.simulate_mode,
            keep_alive_ping_iterval = config.workload.keep_alive_ping_iterval,
            max_lookahead_batches = config.workload.max_lookahead_batches,
            max_prefetch_workers = config.workload.max_prefetch_workers,
            cache_address = config.cache_address,
            shuffle=config.workload.shuffle,
            num_dataset_partitions =config.workload.num_dataset_partitions)
        
        # Create an instance of the coordinator class
        coordinator = SUPERCoordinator(args=super_args)
        
        # Warm up batch creation lambda if not in simulate mode
        if not super_args.simulate_mode:
            logger.info("Warming up batch creation lambda function..")
            response = coordinator.lambda_client.warm_up_lambda(super_args.batch_creation_lambda) 
            if response['success']:
                logger.info(f"Warm up took {response['duration']:.3f}s")
        else:
            logger.info("Running in simualtion mode")

        # Start data loading workers
        logger.info("Data loading workers started")
        # coordinator.prefetch()
        coordinator.start_prefetcher_service()

        # Initialize and start the gRPC server
        cache_service = CacheCoordinatorService(coordinator)
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        cache_coordinator_pb2_grpc.add_CacheCoordinatorServiceServicer_to_server(cache_service, server)
        server.add_insecure_port('[::]:50051')
        server.start()
        logger.info("Server started. Listening on port 50051...")

        # Keep the server running until interrupted
        server.wait_for_termination()
    
    except KeyboardInterrupt:
        logger.info("Server stopped due to keyboard interrupt")
        server.stop(0)
    except Exception as e:
            logger.exception(f"Error in serve(): {e}")
    finally:  
        coordinator.stop_workers()

if __name__ == '__main__':
    serve()
