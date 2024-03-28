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
from utils import format_timestamp,  TokenBucket, remove_trailing_slash
from awsutils import AWSLambdaClient
from queue import  Queue, Empty
import numpy as np

class SUPERCoordinator:
    
    def __init__(self, args:SUPERArgs):
        self.super_args: SUPERArgs = args
        self.dataset:Dataset = Dataset(self.super_args.s3_data_dir)
        self.batch_sampler:BatchSampler = BatchSampler(SequentialSampler(len(self.dataset)), self.super_args.batch_size, self.super_args.drop_last)
        self.epochs: Dict[int, Epoch] = {}
        self.jobs: Dict[int, MLTrainingJob] = {}
        self.lambda_client:AWSLambdaClient = AWSLambdaClient()
        self.prefetch_batches_stop_event = threading.Event()
        self.token_bucket:TokenBucket = TokenBucket(capacity=args.max_lookahead_batches, refill_rate=0)
        self.prefetching_queue = Queue()
        self.active_epoch = self.next_epoch()
        self.executor = futures.ThreadPoolExecutor(max_workers=args.max_prefetch_workers)  # Adjust max_workers as neede
        logger.info(f"Dataset Confirmed. Data Dir: {self.dataset.data_dir}, Total Files: {len(self.dataset)}, Total Batches: {len(self.batch_sampler)}")

    def prefetch(self): 
        while not self.prefetch_batches_stop_event.is_set():
            if self.prefetching_queue.empty():
                    #all batches in the current global epoch have been processed
                    if self.active_epoch.epoch_id < max(self.epochs.keys()):
                        self.active_epoch = max(self.epochs.keys())
                    else:
                        self.active_epoch = self.next_epoch()

                    for batch in self.active_epoch.batches:
                        self.prefetching_queue.put(batch)
            try:
                batch:Batch = self.prefetching_queue.get(timeout=1) # Get the batch from the queue with timeout
                self.token_bucket.wait_for_tokens()
                future = self.executor.submit(self.preftech_bacth, batch)
                if future.result():
                    batch.is_cached = True
                    logger.info(f"Batch {batch.batch_id} prefetch succeeded")
                    self.token_bucket.batch_prefeteched(batch.batch_id)
                else:
                    logger.error(f"Batch {batch.batch_id} prefetch failed")
            except Empty:
                # Queue is empty, continue to the next iteration
                continue

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
        
    def start_workers(self):
        """Starts the prefetching process."""
        try:
            prefetch_thread = threading.Thread(target=self.prefetch, daemon=False, name='prefetch-thread')
            prefetch_thread.start()
        except Exception as e:
            logger.error(f"Error in start_prefetching: {e}")
    
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
    
    def assign_epoch_to_job(self, job:MLTrainingJob):
        if job.current_epoch_id is not None:   
            job.processed_epochs_ids.append(job.current_epoch_id)
        if self.active_epoch.epoch_id not in job.processed_epochs_ids:
            job.prepare_for_new_epoch(self.active_epoch)
        else:
            job.prepare_for_new_epoch(self.next_epoch())


    def next_batch_for_job(self, job_id, num_batches_requested = 1):
        job:MLTrainingJob =  self.jobs[job_id]     
        job.is_active = True

        if job.pending_batches.size < 1: #may go back to change this to size < 2 to set up next epoch before current one ends
            self.assign_epoch_to_job(job)
                    
        num_items = min(job.pending_batches.size, num_batches_requested)
        next_bacthes = job.pending_batches[:num_items].tolist()
        job.pending_batches = job.pending_batches[num_items:]

        for batch in next_bacthes:
            self.token_bucket.batch_accessed(batch_id=batch.batch_id)

        return next_bacthes
    
    def stop_workers(self):
        self.prefetch_batches_stop_event.set()
        self.executor.shutdown(wait=False)

    def next_epoch(self):
        epoch_id = len(self.epochs) + 1
        new_epoch = Epoch(epoch_id)
        for batch in self.batch_sampler:
            new_epoch.add_batch(batch)
            # self.prefetching_queue.put(batch)
        self.epochs[epoch_id] = new_epoch
        return self.epochs[epoch_id]
    
    def handle_job_ended(self, job_id):
        self.jobs[job_id].is_active = False
        self.deactivate_inactive_epochs()
        self.jobs.pop(job_id)

    
    def deactivate_inactive_epochs(self):
        # Collect IDs of active epochs
        active_epoch_ids = {self.active_epoch.epoch_id}
        for job in self.jobs.values():
            if job.is_active and job.current_epoch_id not in active_epoch_ids:
                active_epoch_ids.add(job.current_epoch_id)
        # Deactivate epochs that are not active
        for epoch in self.epochs.values():
            epoch.is_active = epoch.epoch_id in active_epoch_ids


if __name__ == "__main__":
    super_args:SUPERArgs = SUPERArgs()
    coordinator = SUPERCoordinator(super_args)
    job1 = 1
    coordinator.create_new_job(job1,'s3://sdl-cifar10/train/')

    for i in range (0,50):
        batch = coordinator.next_batch_for_job(job1)
        for b in batch:
            logger.info(f'Job {job1}, Batch_indx {i+1}, Batch_id {b.batch_id}')
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