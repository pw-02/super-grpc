from typing import Dict, List
from batch import Batch
import threading
from queue import Queue

class Epoch:
    def __init__(self, epoch_id, partition_id):
        self.epoch_id = epoch_id
        self.partition_id = partition_id
        self.batches: Dict[str, Batch] = {}
        self.batches_finalized = False
        self.is_active = True
        self.pending_batch_accesses:Dict[int, Queue] = {} #job id and queue of batches for that job
        self.lock = threading.Lock()
    
    def queue_up_batches_for_job(self, job_id):
        # Acquire lock to prevent modifications to self.batches
        with self.lock:
            if job_id not in self.pending_batch_accesses:
                self.pending_batch_accesses[job_id] = Queue()
            for batch_id in self.batches.keys(): 
                self.pending_batch_accesses[job_id].put(batch_id)
    
    def add_batch(self, batch: Batch):
        # with self.lock:
            if batch.batch_id not in self.batches:
                self.batches[batch.batch_id] = batch
                # Add new batch to job processing queues
                for job_id in self.pending_batch_accesses.keys():
                    self.pending_batch_accesses[job_id].put(batch.batch_id)
    
                    