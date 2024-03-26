from concurrent.futures import ThreadPoolExecutor
import json
import logging
import sys
from threading import Thread
import queue
import time
import math
from epoch import Epoch
from utils import format_timestamp

class MLTrainingJob():
    
    def __init__(self,job_id):
        self.job_id = job_id
        self.current_epoch = None
        self.is_active= False
        self.processed_epochs = []
        self.pending_batches = []
        self.processed_batches = []
        self.training_speed =  1
    
    def predict_batch_access_time(self, batch_id):
        current_time = time.time()
        if batch_id in self.pending_batches:
            index = self.pending_batches.index(batch_id) + 1
            # predicted_time = current_time + (self.batches_pending_count * (1 / self.training_speed))
            predicted_time = current_time + (index  * (1 / self.training_speed))
        else:
             predicted_time = current_time + math.inf
        return predicted_time

    def has_pending_batches(self):
        return len(self.pending_batches) > 0
    
    def next_batches(self, count=1):
        # Determine the number of batches to retrieve
        num_batches_to_retrieve = min(count, len(self.pending_batches))
        # Retrieve the specified number of batches
        next_batches = self.pending_batches[:num_batches_to_retrieve]     
        # Remove the retrieved batches from pending_batches
        del self.pending_batches[:num_batches_to_retrieve]
        return next_batches