import grpc
import os
import json
import cache_coordinator_pb2 as cache_coordinator_pb2
import cache_coordinator_pb2_grpc as cache_coordinator_pb2_grpc

class SuperClient:
    def __init__(self, server_address='localhost:50051'):
        self.server_address = server_address
        # Establish a connection to the gRPC server
        self.channel = grpc.insecure_channel(server_address)
        # Create a stub (client)
        self.stub = cache_coordinator_pb2_grpc.CacheCoordinatorServiceStub(self.channel)
    
    def ping_server(self):
        job_info = cache_coordinator_pb2.GetPingServerRequest(message='ping')
        try:
            response = self.stub.PingServer(job_info)
            if response.message == 'pong':
                return True, ''
            else:
                return False, 'Unexpected response from server'
        except Exception as e:
            return False, str(e)


    def register_new_job(self, job_id, job_dataset_ids):
        job_info = cache_coordinator_pb2.RegisterJobInfo(job_id=job_id, dataset_ids=job_dataset_ids)
        response = self.stub.RegisterJob(job_info)  
        if response.job_registered:
            pass
            # logger.info(f"Registered Job with Id: '{job_id}'")
            #self.job_id = job_id
        else:
            pass
            #  logger.info(f"Failed to Register Job with Id: '{job_id}'. Server Message: '{response.message}'.")
    
    def get_batch_status(self, batch_id, dataset_id):
            try:
                info = cache_coordinator_pb2.GetBatchStatusRequest(batch_id=batch_id, dataset_id=dataset_id)
                response = self.stub.GetBatchStatus(info)
                return response.batch_cached_or_in_progress
            except Exception as e:
                return False

          

    def register_dataset(self, dataset_id, data_dir, transformations, labelled_samples):
        dataset_info = cache_coordinator_pb2.RegisterDatasetInfo(
            dataset_id=dataset_id,
            data_dir=data_dir,
            transformations = transformations,
            labelled_samples = json.dumps(labelled_samples))
        
        try:
            response = self.stub.RegisterDataset(dataset_info)  
            if response.dataset_registered:
                pass
                # logger.info(f"Registered Dataset with Id: '{dataset_id}'")
            else:
                pass
                # logger.error(f"Failed to Register Dataset with Id: '{dataset_id}'. Server Message: '{response.message}'.")
        except Exception as e:
            pass
            # logger.error(f"Error registering dataset '{dataset_id}': {e}")


    def share_batch_access_pattern(self, job_id, batches:list, dataset_id):
        if self.stub is None:
            raise RuntimeError("Client not initialized. Call create_client() first.")
        batch_access_pattern_list = cache_coordinator_pb2.BatchAccessPatternList(
        job_id=job_id,
        batches=[
            cache_coordinator_pb2.Batch(batch_id=batch[1], sample_indices=batch[0])
            for batch in batches
        ],
        dataset_id=dataset_id)

        # Make the gRPC call
        response = self.stub.ShareBatchAccessPattern(batch_access_pattern_list)
        pass

    def share_job_metrics(self,job_id, dataset_id, metrics:dict):
        if self.stub is None:
            raise RuntimeError("Client not initialized. Call create_client() first.")
        
        job_metrics = cache_coordinator_pb2.JobMetricsInfo(
        job_id=job_id,
        dataset_id = dataset_id,
        metrics = json.dumps(metrics))

        # Make the gRPC call
        response = self.stub.ShareJobMetrics(job_metrics)
        pass

    def __del__(self):
        # Close the gRPC channel when the client is deleted
        self.channel.close()

import multiprocessing
import time
def worker(process_id, server_address):
    # Each worker creates its own instance of CacheCoordinatorClient
    client = SuperClient(server_address)
    while True:     
        # Example: Call GetBatchStatus RPC
        response = client.get_batch_status(process_id, 'example_dataset')
        print(f"Process {process_id}: GetBatchStatus Response - {response}")
        time.sleep(2)

    # Close the gRPC channel when the worker is done
    #del client



def run():
     # Example of using the CacheCoordinatorClient with multiple processes
    server_address = 'localhost:50051'
    num_processes = 4

    # Create a list to hold the process objects
    processes = []

    # Create a super client for the main process
    main_client  = SuperClient(server_address)
    # Example: Call additional RPC with the super client
    
    response = main_client.get_batch_status(123, 'example_dataset')
    print(f"Process Main: GetBatchStatus Response - {response}")
    
    # Fork child processes
    for i in range(0, num_processes):
        process = multiprocessing.Process(target=worker, args=(i, server_address))
        processes.append(process)
        process.start()

    # Wait for all processes to finish
    for process in processes:
        process.join()
   



if __name__ == '__main__':
    run()
