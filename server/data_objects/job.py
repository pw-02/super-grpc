import time
import threading
class Job:
    def __init__(self, job_id,dataset_ids, training_speed=0.1):
        self.job_id = job_id
        self.job_started = time.time()
        self.batches_pending_count = 0  # Difference between batches sent and processed
        self.lock = threading.Lock()  # Lock for synchronizing access
        self.training_speed = training_speed  # Adjust processing speed as needed
        self.dataset_ids = dataset_ids
        self.cache_hits  = 0
        self.cache_misses  = 0
        self.job_active = True
        self.pending_batches = {}
    

    







    def increment_cache_hit_count(self) :
        with self.lock:
            self.cache_hits += 1
    
    def increment_cache_miss_count(self) :
        with self.lock:
            self.cache_misses += 1

    def increment_batches_pending_count(self) :
        with self.lock:
            self.batches_pending_count += 1

    def decrement_batches_prending_count(self):
        with self.lock:
            self.batches_pending_count -= 1

    def predict_batch_access_time(self):
        with self.lock:
            current_time = time.time()
            predicted_time = current_time + (self.batches_pending_count * (1 / self.training_speed))
            return predicted_time
        
    