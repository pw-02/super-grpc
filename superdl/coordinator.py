import time
import json
import threading
from typing import Dict, List
from concurrent import futures

from args import SUPERArgs
from logger_config import logger
from dataset import Dataset
from job import MLTrainingJob, Epoch, Batch
from sampling import BatchSampler, SequentialSampler
from utils import format_timestamp,  CustomQueue, TokenBucket
from awsutils import AWSLambdaClient
from queue import PriorityQueue, Queue, Empty, Full

class SUPERCoordinator:

    def __init__(self, args:SUPERArgs):
        self.super_args: SUPERArgs = args
        self.dataset:Dataset = Dataset(self.super_args.s3_data_dir)
        self.batch_sampler:BatchSampler = BatchSampler(SequentialSampler(len(self.dataset)), self.super_args.batch_size, self.super_args.drop_last)
        self.epochs: Dict[int, Epoch] = {}
        self.jobs: Dict[int, MLTrainingJob] = {}
        self.lambda_client:AWSLambdaClient = AWSLambdaClient()
        self.prefetch_batches_stop_event = threading.Event()
        self.prepared_lookahead_batches: List[str] = []
        self.active_epoch = None
        self.queue_lock = threading.Lock()
        self.batch_queue = Queue()
        self.batch2_queue = CustomQueue(maxsize=args.max_lookahead_batches)
        self.token_bucket:TokenBucket = TokenBucket(capacity=args.max_lookahead_batches, refill_rate=0)
        self.prefetching_queue = Queue()
        self.executor = futures.ThreadPoolExecutor(max_workers=args.max_prefetch_workers)  # Adjust max_workers as needed

    def prefetch(self): 
        while not self.prefetch_batches_stop_event.is_set():
            if self.prefetching_queue.empty():
                    self.active_epoch = self.next_epoch()
            try:
                batch:Batch = self.prefetching_queue.get(timeout=1) # Get the batch from the queue with timeout
                self.token_bucket.wait_for_tokens()
                future = self.executor.submit(self.preftech_bacth, batch)
                if future.result():
                    batch.is_cached = True
                    logger.info(f"Batch {batch.bacth_id} prefetch succeeded")
                    self.token_bucket.batch_prefeteched(batch.bacth_id)
                else:
                    logger.error(f"Batch {batch.bacth_id} prefetch failed")
            except Empty:
                # Queue is empty, continue to the next iteration
                continue

    def preftech_bacth(self, batch:Batch):
        try:
            event_data = {
                'bucket_name': self.dataset.bucket_name,
                'batch_id': batch.bacth_id,
                'batch_metadata': self.dataset.get_samples(batch.indicies),
                }
            #self.lambda_client.invoke_function(self.super_args.batch_creation_lambda,event_data, True)
            return True
        except Exception as e:
            logger.error(f"Error in prefetch_batch: {e}")
            return False
    
    def start_prefetching(self):
        """Starts the prefetching process."""
        try:
            prefetch_thread = threading.Thread(target=self.prefetch, daemon=False)
            prefetch_thread.start()
        except Exception as e:
            logger.error(f"Error in start_prefetching: {e}")


    def create_new_job(self, job_id):
        self.jobs[job_id] = MLTrainingJob(job_id)

    def assign_new_epoch_to_job(self, job_id):
        job:MLTrainingJob = self.jobs[job_id]
        if not job.already_processed_epoch(self.active_epoch.epoch_id):
            job.prepare_new_epoch(self.epochs[self.active_epoch.epoch_id])
    
    def next_batch_accesses_for_job(self, job_id, count = 1):
        job =  self.jobs[job_id]        
        job.is_active = True
        # next_bacthes = []

        if job.pending_batches.empty(): #may go abck to change this to size < 2 to set up next epoch before current one ends
            self.assign_new_epoch_to_job(job.job_id)

        for i in range(0, min(job.pending_batches.qsize(), count)):
            batch:Batch = job.pending_batches.get(block=False)
            # next_bacthes.append(batch)
            self.token_bucket.batch_accessed(bacth_id=batch.bacth_id)
        return batch

    def stop_workers(self):
        self.prefetch_batches_stop_event.set()
        self.executor.shutdown(wait=False)


    def warm_up_lambda(self):
        event_data = {'bucket_name': 'foo','batch_id': 123,'batch_metadata':'foo',}
        response =  self.lambda_client.invoke_function(self.super_args.batch_creation_lambda, json.dumps(event_data), self.super_args.simulate_mode )  # Pass the required payload or input parameters
        if response['StatusCode'] == 200:
            return True
        else:
            return False
        
    def next_epoch(self):
        epoch_id = len(self.epochs) + 1
        new_epoch = Epoch(epoch_id)
        for batch in self.batch_sampler:
            new_epoch.add_batch(batch)
            self.prefetching_queue.put(batch)
        self.epochs[epoch_id] = new_epoch
        return self.epochs[epoch_id]


if __name__ == "__main__":
    super_args:SUPERArgs = SUPERArgs()
    coordinator = SUPERCoordinator(super_args)
    coordinator.start_prefetching()
    time.sleep(1)
    job1 = 1
    coordinator.create_new_job(job1)
    for i in range (0,2):
        batch = coordinator.next_batch_accesses_for_job(job1)
        logger.info(f'Job {job1}, Batch_IDX {i+1}, Batch_id {batch.bacth_id}')
        # coordinator.run_batch_access_predictions()
    time.sleep(4)



   # def prefetch_batches(self):
    #     try:
    #         while not self.prefetch_batches_stop_event.is_set():
    #             # Batch prefetch requests for better efficiency
    #             batch_futures = [self.executor.submit(self.invoke_prefetch_lambda, batch) for batch in self.active_epoch.batches]
    #              # Check the status of futures as they complete
    #             for future, batch_id in zip(batch_futures, self.active_epoch.batch_ids):
    #                 if future.result():
    #                     logger.info(f"Batch {batch_id} prefetch succeeded")
    #                 else:
    #                     logger.error(f"Batch {batch_id} prefetch failed")
    #             self.active_epoch = self.next_epoch()
    #     except Exception as e:
    #          logger.error(f"Error in prefetch_new_batches: {e}")