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
from awsutils import AWSLambdaClient
from queue import  Queue, Empty
import numpy as np
from copy import deepcopy

class SUPERCoordinator:
    
    def __init__(self, args:SUPERArgs):
        self.super_args: SUPERArgs = args
        self.dataset:Dataset = Dataset(self.super_args.s3_data_dir)
        self.epochs: Dict[int, Epoch] = {}
        self.batch_sampler:BatchSampler = BatchSampler(SequentialSampler(len(self.dataset)), self.super_args.batch_size, self.super_args.drop_last)
        self.jobs: Dict[int, MLTrainingJob] = {}
        self.lambda_client:AWSLambdaClient = AWSLambdaClient()
        self.prefetch_batches_stop_event = threading.Event()
        self.token_bucket:TokenBucket = TokenBucket(capacity=args.max_lookahead_batches, refill_rate=0)
        self.executor = futures.ThreadPoolExecutor(max_workers=args.max_prefetch_workers)  # Adjust max_workers as neede
        logger.info(f"Dataset Confirmed. Data Dir: {self.dataset.data_dir}, Total Files: {len(self.dataset)}, Total Batches: {len(self.batch_sampler)}")
    
    def start_workers(self):
        """Starts the prefetching process."""
        try:
            prefetch_thread = threading.Thread(target=self.prefetch, daemon=True, name='prefetch-thread')
            prefetch_thread.start()
        except Exception as e:
            logger.error(f"Error in start_prefetching: {e}")

    def prefetch(self):
        while not self.prefetch_batches_stop_event.is_set():
            self.token_bucket.wait_for_tokens()
            try:
                next_batch:Batch = next(self.batch_sampler)
            except EndOfEpochException:
                self.epochs[self.batch_sampler.epoch_seed].batches_finalized = True
                self.batch_sampler.increment_epoch_seed()
                next_batch:Batch = next(self.batch_sampler)
            future = self.executor.submit(self.preftech_bacth, next_batch)
            if future.result():
                next_batch.is_cached = True
                if next_batch.epoch_seed in self.epochs:
                    self.epochs[next_batch.epoch_seed].add_batch(next_batch)
                else:
                    self.epochs[next_batch.epoch_seed] = Epoch(next_batch.epoch_seed)
                    self.epochs[next_batch.epoch_seed].add_batch(next_batch)
                logger.info(f"Batch {next_batch.batch_id} prefetch succeeded")
                # self.token_bucket.batch_prefeteched(next_batch.batch_id)
            else:
                logger.error(f"Batch {next_batch.batch_id} prefetch failed")
    
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
        if job.current_epoch is not None:   
             job.epoch_history.append(job.current_epoch.epoch_seed)  
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
            self.allocate_epoch_to_job(job)
        
        while job.current_epoch.pending_batch_accesses[job.job_id].qsize() < 1:
            self.token_bucket.capacity +=10
            time.sleep(0.1)
            
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

    # def assign_epoch_to_job(self, job:MLTrainingJob):
    #     if job.active_epoch is not None:   
    #         job.epoch_seed_history.append(job.active_epoch.epoch_seed)  
    #     if self.active_epoch.epoch_seed not in job.epoch_seed_history:
    #         job.prepare_for_new_epoch(self.active_epoch)
    #     else:
    #         job.prepare_for_new_epoch(self.next_epoch())

    def preftech_bacth(self, batch:Batch):
        try:
            event_data = {
                'bucket_name': self.dataset.bucket_name,
                'batch_id': batch.batch_id,
                'batch_metadata': self.dataset.get_samples(batch.indicies),
                }
            #self.lambda_client.invoke_function(self.super_args.batch_creation_lambda,event_data, True)
            return True
        except Exception as e:
            logger.error(f"Error in prefetch_batch: {e}")
            return False
        
    def stop_workers(self):
        self.prefetch_batches_stop_event.set()
        self.executor.shutdown(wait=False)
    
    def handle_job_ended(self, job_id):
        self.jobs[job_id].is_active = False
        self.deactivate_inactive_epochs()
        self.jobs.pop(job_id)

    def deactivate_inactive_epochs(self):
        pass

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
