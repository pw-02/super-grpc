from typing import List, Tuple, Dict
from batch import Batch
from queue import Queue
from sampling import RandomSampler, BatchSampler
from functools import cached_property

class Epoch:
    def __init__(self, epoch_id:int):
        self.is_active:bool =  True
        self.epoch_id:int = epoch_id
        self.epoch_batches: List[Batch] = []
        self.active_batches: Queue[Batch] = Queue()

        # for batch in batch_sampler:
        #     self.epoch_batches.append(batch)
        # self.prefetch_queue.put(batch)
    
    def unload_queue(self):
        while not self.active_batches.empty():
            self.active_batches.get()

    @cached_property
    def batch_ids(self):
        ids = []
        for batch in self.epoch_batches:
            ids.append(batch.bacth_id)
        return ids

    # def cached_batches_count(self):
    #     return sum(1 for batch in self.epoch_batches if batch.is_cached)