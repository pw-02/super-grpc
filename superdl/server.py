import grpc
from concurrent import futures
import proto.cache_coordinator_pb2 as cache_coordinator_pb2
import proto.cache_coordinator_pb2_grpc as cache_coordinator_pb2_grpc
import google.protobuf.empty_pb2
from logger_config import logger
from coordinator import SUPERCoordinator
import hydra
from omegaconf import DictConfig
import sys
from args import SUPERArgs


class CacheCoordinatorService(cache_coordinator_pb2_grpc.CacheCoordinatorServiceServicer):
    def __init__(self, coordinator: SUPERCoordinator):
        self.coordinator = coordinator
    
    def PingServer(self, request, context):
        message = request.message
        return cache_coordinator_pb2.GetPingServerResponse(message = 'pong')
    
    def GetBatchStatus(self, request, context):
        cached_or_inprogress, message = self.coordinator.get_batch_status(request.batch_id,request.dataset_id)
        #logger.info(f"{message}")
        return cache_coordinator_pb2.GetBatchStatusResponse(batch_cached_or_in_progress=cached_or_inprogress, message = message)


    def RegisterJob(self, request, context):
        job_added, message = self.coordinator.add_new_job(request.job_id,request.dataset_ids)
        logger.info(f"{message}")
        return cache_coordinator_pb2.RegisterJobResponse(job_registered=job_added, message = message)
    
    def RegisterDataset(self, request, context):
        dataset_added, message = self.coordinator.add_new_dataset(request.dataset_id,
                                                                    data_dir=request.data_dir,
                                                                    transformations =None if request.transformations == 'null' else request.transformations,
                                                                    labelled_samples= None if request.labelled_samples == 'null' else request.labelled_samples)
        logger.info(f"{message}")
        return cache_coordinator_pb2.RegisterDatasetResponse(dataset_registered=dataset_added, message = message)
    
    def ShareBatchAccessPattern(self, request, context):
        try:
            logger.info(f"Received next {len(request.batches)} batches for job '{request.job_id}'")
            #time.sleep(5)
            self.coordinator.preprocess_new_batches(request.job_id, request.batches, request.dataset_id)
        except Exception as e:
            logger.exception(f"Error processing Batch Access Pattern: {e}")

        return google.protobuf.empty_pb2.Empty()
    
    def ShareJobMetrics(self, request, context):
        try:
            # logger.info(f"Received metrics for job '{request.job_id}'")
            #time.sleep(5)
            self.coordinator.process_job_metrics(request.job_id, request.dataset_id, request.metrics)
        except Exception as e:
            logger.exception(f"Error processing Batch Access Pattern: {e}")

        return google.protobuf.empty_pb2.Empty()
    

@hydra.main(version_base=None, config_path="../conf", config_name="config")
def serve(config: DictConfig):
    try:
        
        logger.info("Starting SUPER Datloading Service")

        super_args:SUPERArgs = SUPERArgs(
            s3_data_dir = config.s3_data_dir,
            batch_creation_lambda = config.batch_creation_lambda,
            batch_size = config.batch_size,
            num_pre_cached_batches = config.num_pre_cached_batches,
            drop_last=config.drop_last,
            simulate_mode = config.simulate_mode
            )
        
        # Create an instance of the Coordinator class
        coordinator = SUPERCoordinator(super_args)

        if not super_args.simulate_mode:
            logger.info("Warming up batch creation lambda..")
            sys.exit()
        
        # logger.info("Starting data loading workers")
        # coordinator.start_workers()








        # if not config_data["testing_locally"]:
        #     logger.info("Warming up batch creation lambda..")
        #     function_working = coordinator.warm_up_function()
        #     if not function_working:
        #         import sys
        #         logger.error(f"Unable to invoke lambda function '{coordinator.lambda_function_name}'. Shutting down!")
        #         sys.exit()
        
        # logger.info("Starting workers..")

        # # Stop the batch processing  workers
        # coordinator.start_workers()
        # # Initialize the CacheCoordinatorService with the Coordinator instance
        # cache_service = CacheCoordinatorService(coordinator)

        # # Start the gRPC server
        # server = grpc.server(futures.ThreadPoolExecutor(max_workers=20))
        # cache_coordinator_pb2_grpc.add_CacheCoordinatorServiceServicer_to_server(cache_service, server)
        # server.add_insecure_port('[::]:50051')
        # server.start()
        # logger.info("Server started. Listening on port 50051...")

        # # Keep the server running until interrupted
        # server.wait_for_termination()
    except KeyboardInterrupt:
        pass
        # server.stop(0)
    except Exception as e:
        logger.exception(f"Error in serve(): {e}")
    finally:  
        # Stop the batch processing  workers
        # coordinator.stop_workers()
        pass
if __name__ == '__main__':
    # Run the server
    serve()
