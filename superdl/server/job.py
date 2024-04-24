from concurrent.futures import ThreadPoolExecutor
import json
import logging
import sys
import time
import math
from sampling import BatchSampler
from utils import format_timestamp
from queue import Queue
from utils import create_unique_id, CustomQueue
from typing import Dict, List
from dataclasses import dataclass
import numpy as np
from copy import deepcopy
import threading

class Batch:
    def __init__(self, batch_indicies, epoch_seed):
        self.indicies: List[int] = batch_indicies
        self.batch_id:str = create_unique_id(self.indicies)
        self.epoch_seed:int = epoch_seed
        self.is_cached:bool = False
        self.caching_in_progress:bool = False
        self.next_access_time:float = None
        self.last_access_time:float = float('inf')
        self.job_access_times = {}  # Dictionary to store job-specific access times
        self.has_been_acessed_before = False
        self.lock = threading.Lock()  # Lock for accessing shared resources

    def update_access_time(self, job_id):
        self.last_accessed_time = time.time()
        self.job_access_times[job_id] = self.last_accessed_time
    
    def is_first_access(self):
        with self.lock:
            if self.has_been_acessed_before:
                return False
            else:
                self.has_been_acessed_before = True
                return True

    def set_cache_status(self, is_cached:bool):
        with self.lock:
            self.is_cached = is_cached


class Epoch:
    def __init__(self, epoch_seed:int):
        self.epoch_seed:int = epoch_seed
        self.batches: Dict[str, Batch] = {}
        self.batches_finalized = False
        self.is_active = True
        self.pending_batch_accesses:Dict[int, Queue] = {} #job id and batch_ids queue for that job
        # Lock for synchronization
        self.lock = threading.Lock()
    
    def queue_up_batches_for_job(self, job_id):

        # Acquire lock to prevent modifications to self.batches
        with self.lock:
            if job_id not in self.pending_batch_accesses:
                self.pending_batch_accesses[job_id] = Queue()
            for batch_id in self.batches.keys(): 
                self.pending_batch_accesses[job_id].put(batch_id)
    
    def add_batch(self, batch: Batch):
        # with self.lock: #lock might not be neccessery, but added for now to be safe
            if batch.batch_id not in self.batches:
                self.batches[batch.batch_id] = batch
                # Add new batch to job processing queues
                for job_id in self.pending_batch_accesses.keys():
                    self.pending_batch_accesses[job_id].put(batch.batch_id)
    

    @property
    def progress(self):
        return 

class MLTrainingJob():
    def __init__(self,job_id:int):
        self.job_id = job_id
        self.is_active=True
        self.processing_rate = 4
        self.current_epoch:Epoch = None
        self.epoch_history = []

    def update_batch_processing_rate(self, rate):
        self.batch_processing_rate = rate
    
    def predict_batch_access_time(self, batch_id):
        current_time = time.time()
        if batch_id in self.pending_batches:
            index = self.pending_batches.index(batch_id) + 1
            # predicted_time = current_time + (self.batches_pending_count * (1 / self.training_speed))
            predicted_time = current_time + (index  * (1 / self.training_speed))
        else:
             predicted_time = current_time + math.inf
        return predicted_time


    def next_batches(self, count=1):
        # Determine the number of batches to retrieve
        num_batches_to_retrieve = min(count, len(self.pending_batches))
        # Retrieve the specified number of batches
        next_batches = self.pending_batches[:num_batches_to_retrieve]     
        # Remove the retrieved batches from pending_batches
        del self.pending_batches[:num_batches_to_retrieve]
        return next_batches
    
