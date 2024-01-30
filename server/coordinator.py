import time
from typing import Dict
from data_objects.job import Job
from data_objects.batch import Batch
from data_objects.dataset import Dataset

from data_objects.priority_queue import PriorityQueue, QueueEmpty
import threading
import requests
import boto3
import json
from concurrent import futures
from logger_config import logger

DEV_MODE = True

def format_timestamp(current_timestamp, use_utc=True):
    if use_utc:
        time_struct = time.gmtime(current_timestamp)
    else:
        time_struct = time.localtime(current_timestamp)

    formatted_current_time = time.strftime("%Y-%m-%d %H:%M:%S", time_struct)
    return formatted_current_time



class Coordinator:


    def __init__(self, lambda_function_name, s3_bucket_name,testing_locally,sam_local_url,sam_local_endpoint  ):
        self.registered_jobs: Dict[int, Job] = {}  # Dictionary to store job information
        self.registered_datasets: Dict[int, Dataset] = {}

        self.batch_pqueue = PriorityQueue()  # Priority queue for batch processing using heapq
        self.s3_bucket_name = s3_bucket_name
        self.testing_locally = testing_locally
        self.sam_local_url = sam_local_url
        self.sam_local_endpoint = sam_local_endpoint
        self.lambda_function_name = lambda_function_name
       
        if not self.testing_locally:
            try:
                self.lambda_client = boto3.client('lambda')
                #add a function call in here to wake up the lambda
            except Exception as e:
                logger.error(f"Error initializing Lambda client: {e}")

        # Initialize thread pool executors
        self.pre_process_executor = futures.ThreadPoolExecutor(max_workers=1)
        self.processing_executor =  futures.ThreadPoolExecutor(max_workers=7) #this must always be at least 2
        self.post_processing_executor =  futures.ThreadPoolExecutor(max_workers=1)
        self.dequeuing_stop_event = threading.Event()
        self.lock = threading.Lock()  # Added lock for thread safety
        
    def get_batch_status(self, batch_id, dataset_id):
        batch = self.registered_datasets[dataset_id].batches[batch_id]
        
        if batch.is_cached or batch.caching_in_progress:
            message = f"Batch'{batch_id}' is cached or in actively being cached"
            return True,  message
        else:
            message = f"Batch'{batch_id}' is not cached or actively being cached"
            return False,  message
            

    def add_new_job(self,job_id, dataset_ids):
        try:  
            if job_id not in self.registered_jobs:
                self.registered_jobs[job_id] = Job(job_id, list(dataset_ids))
                return True, f"New Job Registered. Job Id:'{job_id}', Datasets: {list(dataset_ids)}"

            else:
                return False, f"Skipped Job '{job_id}' Registration. Already Registered"
        except Exception as e:
            message = f"Error in add_new_job: {e}"
            logger.error(message)
            return False,  message
    
    
    def add_new_dataset(self, dataset_id, source_system, data_dir, labelled_samples):
        try:
            if dataset_id in self.registered_datasets:
                return True, f"Skipped Dataset '{dataset_id}' Registration. Already Registered"
            else:
                dataset = Dataset(dataset_id, source_system, data_dir, labelled_samples)
                if len(dataset) > 1:
                    with self.lock:  # Use lock to ensure thread safety
                        self.registered_datasets[dataset_id] = dataset
                    return True, f"New Dataset Registered. Dataset Id: '{dataset_id}'"
                else:
                    message = f"No data found for dataset '{data_dir}' in '{source_system}'"
                    return False,  message   
        except Exception as e:
            message = f"Error in add_new_dataset: {e}"
            logger.error(message)
            return False,  message
        

    def preprocess_new_batches(self, job_id, batches, dataset_id):
        try:
            # Define a function to be executed asynchronously
            def prepocess_batch_async(job_id, batch_id, batch_sample_indices, dataset_id):

                # Increment the count of batches pending for the job
                self.registered_jobs[job_id].increment_batches_pending_count()

                # Predict the time when the batch will be accessed by the job
                predicted_time = self.registered_jobs[job_id].predict_batch_access_time()
                
                # Check if we have seen this batch_id before, if not add it
                dataset_batches = self.registered_datasets[dataset_id].batches

                if batch_id not in self.registered_datasets[dataset_id].batches:
                    dataset_batches[batch_id] = Batch(batch_id, batch_sample_indices)

                dataset_batches[batch_id].predicted_access_times[job_id] = predicted_time
                
                #Update next access time if the predicted time is earlier than the current next access time
                next_access_time = dataset_batches[batch_id].update_next_access_time(predicted_time)
                
                logger.info(f"Pre-processed batch '{batch_id}' for job '{job_id}'. Samples:{len(batch_sample_indices)}, Job Access Time {format_timestamp(predicted_time)}, Next Access Time {format_timestamp(next_access_time)} ")

                # Enqueue a batch with its job_id, batch_id, using next_access_time as priority
                self.batch_pqueue.enqueue(next_access_time, (job_id, dataset_id, batch_id))

            # Submit the function for asynchronous execution
            for batch in batches:                
                self.pre_process_executor.submit(prepocess_batch_async(job_id, batch.batch_id, list(batch.sample_indices), dataset_id))

        except Exception as e:
            logger.error(f"Error in preprocess_new_batch: {e}")

    def batch_processor_worker(self):
        try:
            while not self.dequeuing_stop_event.is_set():
                # Dequeue items from the priority queue with a timeout
                try:
                    priority, item = self.batch_pqueue.dequeue()  # Adjust the timeout as needed
                except QueueEmpty:
                    # Handle the case when the queue is empty
                    #logger.info("Batch Processor Sleeping for 0.1s - Queue Empty")
                    time.sleep(0.01)
                    continue  # Continue to the next iteration

                job_id, dataset_id, batch_id = item
                # Use a ThreadPoolExecutor to process the item
                # logger.info(f"Dequeued Batch for Processing. Batch Id: {batch_id}, Access Time: {priority}")

                self.processing_executor.submit(self.process_batch, job_id, batch_id, dataset_id)
        except Exception as e:
            logger.error(f"Error in worker_function: {e}")
    
   
    def start_workers(self):
        self.processing_executor.submit(self.batch_processor_worker)

    def stop_workers(self):
        self.dequeuing_stop_event.set()
        self.pre_process_executor.shutdown(wait=False)
        self.processing_executor.shutdown(wait=False)
        self.post_processing_executor.shutdown(wait=False)

    def process_batch(self, job_id, batch_id, dataset_id):
        try:
            batch = self.registered_datasets[dataset_id].batches[batch_id]

            if not batch.is_cached and not batch.caching_in_progress:
                action = "Cached Batch"
            elif (time.time() - batch.last_accessed > 900) and not batch.caching_in_progress:
                action = "Keep-Alive-Ping"
            else:
                action = "Skipped"
                logger.info(f"Processed batch '{batch_id}'. Action Taken: '{action}', Message: 'Batch already cached or actively being cached by another process'")
                return

            samples = self.registered_datasets[dataset_id].get_samples_for_batch(batch.batch_sample_indices)
            batch.set_caching_in_progress(True)

            successfully_cached, error_message = self.prefetch_batch( batch_id, samples, check_cache_first=not (action == "Cached Batch"))

            if successfully_cached:
                batch.set_cached_status(is_cached=True)
                logger.info(f"Processed Batch '{batch_id}'. Action Taken: '{action}'" if action == "Cached Batch" else f"Processed batch '{batch_id}'. Action Taken: '{action}', Message: 'Cached previously, but not accessed in a while'")
            else:
                logger.error(f"Failed to Processing Batch '{batch_id}'. Message: '{error_message}'")

            batch.set_caching_in_progress(False)

        except Exception as e:
            logger.error(f"Error in process_batch: {e}")

    

    def prefetch_batch(self, batch_id, labelled_samples, check_cache_first):
        try:

            event_data = {
                'bucket_name': self.s3_bucket_name,
                'batch_id': batch_id,
                'batch_metadata': labelled_samples,  # Replace with actual batch metadata
            }

            if self.testing_locally:
                response = requests.post(f"{self.sam_local_url}{self.sam_local_endpoint}", json=event_data)
            else:
                response =  self.lambda_client.invoke(FunctionName=self.lambda_function_name,
                                                     #InvocationType='Event',  # Change this based on your requirements
                                                     Payload=json.dumps(event_data)  # Pass the required payload or input parameters
                                                     )
            # Check the response status code
            if response['StatusCode'] == 200:
                return True, f"Sucessfully cached batch: {batch_id}"
            else:
                return False, f"Error invoking function. Status code: {response.status_code}"
            
        except Exception as e:
            return False, f"Exception invoking function.{e}"
        

    def process_job_metrics(self, job_id, dataset_id, metrics):
        try:
            # Define a function to be executed asynchronously
            def process_metrics_async(job_id, dataset_id, batch_id, metrics:Dict):
                job = self.registered_jobs[job_id]
                #reduce number of pending batches processed
                job.decrement_batches_prending_count()
                #update training speed
                job.training_speed = metrics['training_speed']

                cache_hit =  metrics['cache_hit']
                if cache_hit:
                    job.increment_cache_hit_count()
                else:
                    job.increment_cache_miss_count()

                #get prediction access time
                batch_access_time = metrics['access_time']

                batch = self.registered_datasets[dataset_id].batches[batch_id]

                batch.actual_access_times[job_id] = batch_access_time

                predicted_time = batch.predicted_access_times[job_id]
                mae = abs(predicted_time - batch_access_time)

                # Check if MAE is not None before reporting
                if mae is not None:
                    # logger.info(f"Mean Absolute Error for Sample {job_id} in Batch {batch_id}: {mae}")
                    logger.info(f"Job:{job_id}, Batch Id:{batch_id}, Predited:{format_timestamp(predicted_time)}, Actual:{format_timestamp(batch_access_time)}, MAE:{mae}, Cache Hit:{cache_hit}")

            # Submit the function for asynchronous execution
            metrics = json.loads(metrics)
            batch_id = metrics['batch_id']
            self.pre_process_executor.submit(process_metrics_async(job_id,  dataset_id,batch_id, metrics))

        except Exception as e:
            logger.error(f"Error in process_training_metrics: {e}")

