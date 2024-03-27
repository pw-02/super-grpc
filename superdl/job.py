from concurrent.futures import ThreadPoolExecutor
import json
import logging
import sys
import time
import math
from utils import format_timestamp
from queue import Queue

from utils import create_unique_id, CustomQueue
from typing import Dict, List
from dataclasses import dataclass

class Batch:
    def __init__(self, batch_indicies):
        self.indicies: List[int] = batch_indicies
        self.bacth_id:str = create_unique_id(self.indicies)
        self.is_cached:bool = False
        self.caching_in_progress:bool = False
        self.next_access_time:float = None
        self.last_access_time:float = float('inf')
        self.parent_epoch_id:int = None

class Epoch:
    def __init__(self, epoch_id:int):
        self.epoch_id:int = epoch_id
        self.batches: List[Batch] = []
        self.batch_ids: List[str] = []

    def add_batch(self, batch:Batch):
        self.batches.append(batch)
        self.batch_ids.append(batch.bacth_id)
    
    @property
    def progress(self):
        return 

class MLTrainingJob():
    def __init__(self,job_id):
        self.job_id = job_id
        self.current_epoch_id = None
        self.processed_epochs: List[int] = []
        self.pending_batches: CustomQueue[Batch] = CustomQueue()
        self.is_active= False
        self.training_speed =  1

    def already_processed_epoch(self, epoch_id):
        return epoch_id in self.processed_epochs
    
    def prepare_new_epoch(self, epoch:Epoch):
        self.current_epoch_id = epoch.epoch_id
        for batch in epoch.batches:
            self.pending_batches.put(batch)
    


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