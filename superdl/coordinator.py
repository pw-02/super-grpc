import time
from typing import Dict, List
from args import SUPERArgs
from concurrent import futures
from logger_config import logger
from dataset import Dataset
from job import MLTrainingJob
from epoch import Epoch
from batch import Batch
from sampling import RandomSampler, BatchSampler, SequentialSampler
from statistics import mode
import copy
import math
from utils import format_timestamp
from awsutils import AWSLambdaClient
import json
import threading

class SUPERCoordinator:
    
    def __init__(self, args:SUPERArgs):
        self.super_args: SUPERArgs = args
        self.dataset:Dataset = Dataset(self.super_args.s3_data_dir)
        self.batch_sampler:BatchSampler = BatchSampler(SequentialSampler(len(self.dataset)), self.super_args.batch_size, self.super_args.drop_last)
        self.epochs: Dict[int, Epoch] = {}
        self.jobs: Dict[int, MLTrainingJob] = {}
        self.lambda_client:AWSLambdaClient = AWSLambdaClient()
        self.batch_prep_stop_event = threading.Event()

    
    @property
    def active_epochs(self)->Dict[int, List[int]]:
        active_epochs: Dict[int, List[int]] = {}
        for job in self.jobs.values():
            if job.is_active: #and job.current_epoch not in active_epoch_ids
                if job.current_epoch not in active_epochs:
                    active_epochs[job.current_epoch] = [job.job_id]
                else:
                    active_epochs[job.current_epoch].append(job.job_id)
        return active_epochs
    
    def start_workers(self):
        pass
        # self.processing_executor.submit(self.batch_processor_worker)

    def stop_workers(self):
        self.batch_prep_stop_event.set()
        # self.pre_process_executor.shutdown(wait=False)
        # self.processing_executor.shutdown(wait=False)
        # self.post_processing_executor.shutdown(wait=False)
    
    def gen_new_epoch(self):
        epoch_id = len(self.epochs) + 1
        self.epochs[epoch_id] = Epoch(epoch_id)
        for batch in self.batch_sampler:
            self.epochs[epoch_id].epoch_batches.append(batch)
        return self.epochs[epoch_id]
    
    def new_job(self, job_id):
        self.jobs[job_id] = MLTrainingJob(job_id)

    def assign_epoch_batches_to_job(self, job_id):  
        epoch = None       
        #find all active epochs across jobs   
        active_epoch_ids = []
        for job in self.jobs.values():
            if job.is_active: #and job.current_epoch not in active_epoch_ids
                active_epoch_ids.append(job.current_epoch) if job.current_epoch is not None else None

        # Find unprocessed active epochs for the specific job
        unprocessed_active_epoch_ids = set(active_epoch_ids) - set(self.jobs[job_id].processed_epochs)

        if len(unprocessed_active_epoch_ids) < 1:
           epoch = self.gen_new_epoch()
        else:
            highest_id = mode(unprocessed_active_epoch_ids)
            epoch = self.epochs[highest_id]

        self.jobs[job_id].pending_batches = copy.deepcopy(epoch.batch_ids)
        self.jobs[job_id].current_epoch = epoch.epoch_id
      
    def next_batches_for_job(self, job_id):
        job =  self.jobs[job_id]        
        job.is_active = True
        if not job.has_pending_batches(): #all batches processed for the current epoch
            job.processed_epochs.append(job.current_epoch) if job.current_epoch is not None else None
            self.assign_epoch_batches_to_job(job_id)
        return job.next_batches(count=1)
    
    def run_batch_access_predictions(self, interval_seconds=5):
        # while True:
        for epoch_id, jobs in self.active_epochs.items():
            for bacth in self.epochs[epoch_id].epoch_batches:
                min_next_access_time = math.inf 
                for job_id in jobs:
                    min_next_access_time = min(min_next_access_time, self.jobs[job_id].predict_batch_access_time(bacth.bacth_id))
                    logger.info(f"Job '{job_id}' predicted accessing batch '{bacth.bacth_id} at {format_timestamp(min_next_access_time)}")
                bacth.set_next_access_time(min_next_access_time)
                if min_next_access_time is not math.inf:
                    self.epochs[epoch_id].active_batches.put(bacth)

            # Wait for the specified interval before running again
            # time.sleep(interval_seconds)
    
    
                
    def run_prepare_batchs(self):

        while not self.batch_prep_stop_event.is_set():

            for epoch_id in self.active_epochs:
                while not self.epochs[epoch_id].active_batches.empty():

                    batch:Batch = self.epochs[epoch_id].active_batches.get()

                    if not batch.is_cached and not batch.caching_in_progress:
                        batch.caching_in_progress = True
                        action = "prefetch"
                    elif (time.time() - batch.last_access_time > self.super_args.keep_alive_ping_iterval) and not batch.caching_in_progress:
                        batch.caching_in_progress = True
                        action = "ping"
                    else:
                        action = "skip"
                        logger.info(f"Processed batch '{batch.bacth_id}'. Action Taken: '{action}', Message: 'Batch already cached or actively being cached by another process'")
                        return
                
                samples = self.dataset.get_samples(batch.indicies)
                successfully_cached, error_message = self.prefetch_batch(batch_id, samples, None, check_cache_first=not (action == "Cached Batch"))

                if successfully_cached:
                    batch.is_cached = True
                    logger.info(f"Processed Batch '{batch_id}'. Action Taken: '{action}'" if action == "Cached Batch" else f"Processed batch '{batch_id}'. Action Taken: '{action}', Message: 'Cached previously, but not accessed in a while'")
                else:
                    logger.error(f"Failed to Processing Batch '{batch_id}'. Message: '{error_message}'")

                batch.caching_in_progress(False)
        
    def prefetch_batch(self, batch_id, labelled_samples, transformations, check_cache_first):
        event_data = {
            'bucket_name': self.dataset.bucket_name,
            'batch_id': batch_id,
            'batch_metadata': labelled_samples,
            }
        if transformations is not None:
            event_data['transformations'] = transformations

        response = self.lambda_client.invoke_function(self.super_args.batch_creation_lambda,json.dumps(event_data), self.super_args.simulate_mode)
        response_payload = json.loads(response['Payload'].read().decode('utf-8'))
        return response_payload['is_cached'], response_payload['message']
    
    def warm_up_lambda(self):
        event_data = {'bucket_name': 'foo','batch_id': 123,'batch_metadata':'foo',}
        response =  self.lambda_client.invoke_function(self.super_args.batch_creation_lambda, json.dumps(event_data), self.super_args.simulate_mode )  # Pass the required payload or input parameters
        if response['StatusCode'] == 200:
            return True
        else:
            return False
        
        
if __name__ == "__main__":
    super_args:SUPERArgs = SUPERArgs()
    coordinator = SUPERCoordinator(super_args)
    job1 = 1
    coordinator.new_job(job1)

    for i in range (0,8):
        batch_id = coordinator.next_batches_for_job(job1)
        print(f'Job {job1}, Batch_IDX {i+1}, Batch_id {batch_id}')
        coordinator.run_batch_access_predictions()

  


