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
from epoch import Epoch
from logger_config import logger



class MLTrainingJob():
    def __init__(self,job_id:int):
        self.job_id = job_id
        self.is_active=True
        self.processing_rate = 1
        self.epoch_history = []
        self.epochs_processed = 0
        self.partition_epochs_remaining = []
        self.current_partition_epoch:Epoch = None

        # Variables to track the previous request time and calculate average time interval
        self.previous_request_time = None
        self.total_time_intervals = 0
        self.num_intervals = 0
    
    def reset_partition_epochs_remaining(self, partition_ids):
        for id in partition_ids:
            self.partition_epochs_remaining.append(id)

    def update_batch_processing_rate(self):
        # Calculate the average time interval between requests
        current_request_time = time.time() 
        if self.previous_request_time is not None:
            # Calculate the time interval between this request and the previous request
            time_interval = current_request_time - self.previous_request_time 
            # Update the total time intervals and number of intervals
            self.total_time_intervals += time_interval
            self.num_intervals += 1
        # Update the previous request time to the current request time
        self.previous_request_time = current_request_time    
        # Calculate the average time interval if there have been intervals recorded
        if self.num_intervals > 0:
            average_time_interval = self.total_time_intervals / self.num_intervals
            self.processing_rate = average_time_interval
            logger.info(f"Average time interval between requests: {average_time_interval:.4f} seconds")
        
    # def predict_batch_access_time(self, batch_id):
    #     current_time = time.time()
    #     if batch_id in self.pending_batches:
    #         index = self.pending_batches.index(batch_id) + 1
    #         # predicted_time = current_time + (self.batches_pending_count * (1 / self.training_speed))
    #         predicted_time = current_time + (index  * (1 / self.training_speed))
    #     else:
    #          predicted_time = current_time + math.inf
    #     return predicted_time


    def next_batches(self, count=1):
        # Determine the number of batches to retrieve
        num_batches_to_retrieve = min(count, len(self.pending_batches))
        # Retrieve the specified number of batches
        next_batches = self.pending_batches[:num_batches_to_retrieve]     
        # Remove the retrieved batches from pending_batches
        del self.pending_batches[:num_batches_to_retrieve]
        return next_batches
    
