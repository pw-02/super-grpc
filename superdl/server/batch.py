
from typing import Dict, List
import threading
from utils import create_unique_id
import time

class Batch:
    def __init__(self, batch_indicies, epoch_seed, idx):
        self.indicies: List[int] = batch_indicies
        self.batch_id:str = f"{epoch_seed}_{idx}_{create_unique_id(self.indicies)}" #create_unique_id(self.indicies)
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

