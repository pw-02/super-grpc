import time
import json
import threading
from typing import Dict, List
from concurrent import futures
from args import SUPERArgs
from logger_config import logger
from dataset import Dataset
from job import MLTrainingJob, Epoch, Batch
from sampling import BatchSampler, SequentialSampler, EndOfEpochException, RandomSampler
from utils import format_timestamp,  TokenBucket, remove_trailing_slash
from aws_utils import AWSLambdaClient
from queue import  Queue, Empty
import numpy as np
from copy import deepcopy
from concurrent.futures import ThreadPoolExecutor

class SUPERCoordinator:

    def __init__(self, args:SUPERArgs):
        self.super_args: SUPERArgs = args
        self.dataset:Dataset = Dataset(self.super_args.s3_data_dir)
        self.epochs: Dict[int, Epoch] = {}
        self.batch_sampler:BatchSampler = BatchSampler(RandomSampler(len(self.dataset)), self.super_args.batch_size, self.super_args.drop_last)
        self.jobs: Dict[int, MLTrainingJob] = {}
        self.lambda_client:AWSLambdaClient = AWSLambdaClient()
        self.prefetch_batches_stop_event = threading.Event()
        self.token_bucket:TokenBucket = TokenBucket(capacity=args.max_lookahead_batches, refill_rate=0)
        self.executor = ThreadPoolExecutor(max_workers=args.max_prefetch_workers)  # Adjust max_workers as neede
        logger.info(f"Dataset Confirmed. Data Dir: {self.dataset.data_dir}, Total Files: {len(self.dataset)}, Total Batches: {len(self.batch_sampler)}")

    def start_prefetcher_service(self):
        """Starts the prefetching process."""
        try:
            prefetch_thread = threading.Thread(target=self.prefetch, daemon=True, name='prefetch-thread')
            prefetch_thread.start()
        except Exception as e:
            logger.error(f"Error in start_prefetching: {e}")

    def prefetch(self): 
        # Process items in a loop while the stop event is not set
        while not self.prefetch_batches_stop_event.is_set():
            self.token_bucket.wait_for_tokens()
            try:
                next_batch = next(self.batch_sampler)
                self.epochs.setdefault(next_batch.epoch_seed, Epoch(next_batch.epoch_seed)).add_batch(next_batch)           
                self.executor.submit(self.prefetch_batch, next_batch)
            except EndOfEpochException:
                self.epochs[self.batch_sampler.epoch_seed].batches_finalized = True
                self.batch_sampler.increment_epoch_seed()
        
    def get_dataset_info(self, data_dir):
        num_files = len(self.dataset)
        num_chunks = len(self.batch_sampler)
        chunk_size = self.super_args.batch_size
        return num_files, num_chunks, chunk_size
        
    def create_new_job(self, job_id, data_dir):
        if job_id in self.jobs:
            message = f"Job with id '{job_id}' already registered. Skipping."
            success  = False
        elif remove_trailing_slash(data_dir).casefold() != remove_trailing_slash(self.dataset.data_dir).casefold():
            success  = False
            message = f"Failed to register job with id '{job_id}' because data dir '{data_dir}' was not found in SUPER."
        else:
            self.jobs[job_id] = MLTrainingJob(job_id)
            message = f"New job with id '{job_id}' successfully registered."
            success  = True   
        return success, message
    
    def allocate_epoch_to_job(self, job:MLTrainingJob):

        #find an epoch that has a high percentage of cached bacthes
        #batches are flagged if they have not been access in 15min
        

        epoch:Epoch = self.epochs[list(self.epochs.keys())[-1]]
        epoch.queue_up_batches_for_job(job.job_id)
        job.current_epoch = epoch

    def next_batch_for_job(self, job_id, num_batches_requested = 1): 
        # get the next round of batches for processing for the given job
        job:MLTrainingJob = self.jobs[job_id]
        if job.current_epoch is None:
            self.allocate_epoch_to_job(job)
        elif job.current_epoch.batches_finalized and job.current_epoch.pending_batch_accesses[job.job_id].empty():
            #job finished its current epoch
            job.epoch_history.append(job.current_epoch.epoch_seed)  
            self.allocate_epoch_to_job(job)
        
        while job.current_epoch.pending_batch_accesses[job.job_id].qsize() < 1:
            # self.token_bucket.capacity +=10
            time.sleep(0.01)
            
        num_items = min(job.current_epoch.pending_batch_accesses[job.job_id].qsize(), num_batches_requested)
        next_batches = []

        for i in range(0, num_items):
            batch_id = job.current_epoch.pending_batch_accesses[job.job_id].get()
            batch:Batch = job.current_epoch.batches[batch_id]
            self.token_bucket.refill(1) if batch.is_first_access() else None
            # self.token_bucket.batch_accessed(batch_id) #adds a new token to the bucket if the first time this batch is accessed
            next_batches.append(batch)
        # self.token_bucket.batch_accessed(batch_ids) #adds a new token to the bucket if the first time this batch is accessed
        return next_batches


    def prefetch_batch(self, next_batch:Batch, attempt = 1):
        if attempt >  5:
            return True
        try:
            payload = {
                'bucket_name': self.dataset.bucket_name,
                'batch_id': next_batch.batch_id,
                'batch_samples': self.dataset.get_samples(next_batch.indicies),
                'task':'vision',
                'cache_address': self.super_args.cache_address
                }
            response = self.lambda_client.invoke_function(self.super_args.batch_creation_lambda,json.dumps(payload), self.super_args.simulate_mode)

            if response['success'] == True:
                # logger.info(f"{response['message']}. Request Duration: {response['duration']:.3f}s")
                logger.info(f"Cached Batch_Id: {next_batch.batch_id}, Request Duration: {response['duration']:.3f}s")
                next_batch.set_cache_status(is_cached=True)
            else:
                logger.info(f" Failed to cache  Batch_Id: {next_batch.batch_id}, Message: {response['message']}, Request Duration: {response['duration']:.3f}s, Attempt: {attempt}")
                attempt +=1
                self.prefetch_batch(next_batch,attempt)
            return True
        except Exception as e:
            logger.error(f"Error in prefetch_batch: {e}")
            return False
        
    def stop_workers(self):
        self.prefetch_batches_stop_event.set()
        # self.executor.shutdown(wait=False)
    
    def handle_job_ended(self, job_id):
        self.jobs[job_id].is_active = False
        self.deactivate_inactive_epochs()
        self.jobs.pop(job_id)

    def deactivate_inactive_epochs(self):
        # # Collect IDs of active epochs
        # active_epoch_ids = {self.active_epoch.epoch_id}
        # for job in self.jobs.values():
        #     if job.is_active and job.current_epoch_id not in active_epoch_ids:
        #         active_epoch_ids.add(job.current_epoch_id)
        # # Deactivate epochs that are not active
        # for epoch in self.epochs.values():
        #     epoch.is_active = epoch.epoch_id in active_epoch_ids
        pass

    def test_rate(self, num_items_to_process = 10, num_workers = 10):
        from concurrent.futures import ThreadPoolExecutor, as_completed

        # Use a thread pool to invoke the Lambda function for each item in the iterator
        with ThreadPoolExecutor(max_workers=num_workers) as executor:
            start_time = time.time()            
            # Create a list to hold the futures (tasks)
            futures = []
            item_count = 0  # Initialize item count
            for item in self.batch_sampler:
                # Submit a task to invoke the Lambda function for the current item
                futures.append(executor.submit(self.prefetch_batch, item))
                item_count += 1
                # Stop processing once 40 items have been processed
                if item_count >= num_items_to_process:
                    break
            # Collect results as they complete
            for future in as_completed(futures):
                result = future.result()
                # Optionally, process the result further if needed
                print(f"Lambda response: {result}")
            
            # End timing
            end_time = time.time()
            
        # Calculate elapsed time
        elapsed_time = end_time - start_time

        # Print the elapsed time
        print(f"Elapsed time for processing {num_items_to_process} items from the iterator: {elapsed_time:.2f} seconds")


       
 
if __name__ == "__main__":
    super_args:SUPERArgs = SUPERArgs()
    coordinator = SUPERCoordinator(super_args)
    # Start worker threads
    coordinator.start_workers()
    time.sleep(2)
    try:
        job1 = 1
        coordinator.create_new_job(job1, 's3://sdl-cifar10/train/')

        for i in range(0, 10):
            batch = coordinator.next_batch_for_job(job1)
            for b in batch:
                logger.info(f'Job {job1}, Batch_index {i+1}, Batch_id {b.batch_id}')
            time.sleep(1)
        
         # Infinite loop to keep the program running
        while True:
            # Do nothing or perform some tasks here
            pass

            # coordinator.run_batch_access_predictions()
    finally:
        # Stop worker threads before exiting
        coordinator.stop_workers()
