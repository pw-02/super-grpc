import grpc
import os
import json
import cache_coordinator_pb2 as cache_coordinator_pb2
import cache_coordinator_pb2_grpc as cache_coordinator_pb2_grpc

class SuperClient:
    def __init__(self, server_address='localhost:50051'):
        self.stub = self.create_client(server_address)
        #self.job_id = job_id


    def create_client(self, server_address):
        # Create a gRPC channel
        channel = grpc.insecure_channel(server_address)

        # Create a gRPC stub
        return cache_coordinator_pb2_grpc.CacheCoordinatorServiceStub(channel)
    
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
        info = cache_coordinator_pb2.GetBatchStatusRequest(batch_id=batch_id, dataset_id=dataset_id)
        response = self.stub.GetBatchStatus(info)
        return  response.batch_cached_or_in_progress
      

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


def run_client():
    # Create a gRPC client
    client = SuperClient()
    client.create_client('localhost:50051')

    job_id = os.getpid()

       # Use client to send batch access pattern
    batch_list = [(1,[1, 2, 3]), (2,[4, 5, 6]), (3,[7, 8, 9])]
    client.share_batch_access_pattern(job_id, batch_list, 'val')

if __name__ == '__main__':
    run_client()
