import grpc
import proto.cache_coordinator_pb2 as cache_coordinator_pb2
import proto.cache_coordinator_pb2_grpc as cache_coordinator_pb2_grpc
from logger_config import logger
import time

def run_unit_tests():
    # Connect to the gRPC server
    channel = grpc.insecure_channel('localhost:50051')
    stub = cache_coordinator_pb2_grpc.CacheCoordinatorServiceStub(channel)
    
    # Ping the server
    ping_response = stub.Ping(cache_coordinator_pb2.PingRequest(message='ping'))
    logger.info(f"Ping Response: {ping_response.message}")

    # Register a job
    register_job_response = stub.RegisterJob(cache_coordinator_pb2.RegisterJobRequest(job_id=123, data_dir='s3://sdl-cifar10/train/'))
    logger.info(f"Register Job Response: {register_job_response.job_registered}, {register_job_response.message}")

    # DatasetInfo
    data_info_response = stub.GetDatasetInfo(cache_coordinator_pb2.DatasetInfoRequest(data_dir='s3://sdl-cifar10/train/'))
    logger.info(f"Dataset Info Response: Num Files={data_info_response.num_files}, Num Chunks={data_info_response.num_chunks}, Chunk Size={data_info_response.chunk_size} ")


    # Request next batch to process
    next_batch_response = stub.GetNextBatchToProcess(cache_coordinator_pb2.GetNextBatchRequest(job_id=123, num_batches_requested=1))
    for batch in next_batch_response.batches:
        logger.info(f"Received batch: {batch.batch_id}, {batch.indicies}, {batch.is_cached}")
    
    # Notify job ended
    stub.JobEnded(cache_coordinator_pb2.JobEndedRequest(job_id=123))
    logger.info("Job Ended notification sent")

def request_batches_test(num_epochs = 2, epoch_size=50000, batches_per_request=16):
    # Connect to the gRPC server
    channel = grpc.insecure_channel('localhost:50051')
    stub = cache_coordinator_pb2_grpc.CacheCoordinatorServiceStub(channel)

    # Register a job
    register_job_response = stub.RegisterJob(cache_coordinator_pb2.RegisterJobRequest(job_id=123, data_dir='s3://sdl-cifar10/train/'))
    logger.info(f"Register Job Response: {register_job_response.job_registered}, {register_job_response.message}")

    start_time = time.time()
    end = time.time()
    for e in range(0, num_epochs):
        indicies_count = 0
        request_counter  = 0
        batch_count =0

        while indicies_count < epoch_size:
            # time.sleep(0.2)
            next_batch_response = stub.GetNextBatchToProcess(cache_coordinator_pb2.GetNextBatchRequest(job_id=123, num_batches_requested=batches_per_request))
            request_counter +=1
            for batch in next_batch_response.batches:
                indicies_count += len(batch.indicies)
                batch_count +=1
            logger.info(f"{request_counter}-{len(next_batch_response.batches)}-{indicies_count}")
                # logger.info(f"{i+1} - Received batch: {batch.batch_id}, Size: {len(batch.indicies)}")
        epoch_time = time.time() - end
        logger.info(f"Epoch: {e+1}, Items: {indicies_count}, Batches: {batch_count}, Time: {epoch_time} seconds, Rate: {batch_count/epoch_time} batches/sec")
        end = time.time()
    end_time = time.time()
    execution_time = end_time - start_time
    logger.info(f"Total execution time: {execution_time} seconds")

if __name__ == '__main__':
    run_unit_tests()
    request_batches_test(num_epochs = 1, epoch_size=50000, batches_per_request=1)
    
