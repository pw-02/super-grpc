import time
import threading

class Batch:
    def __init__(self, batch_id, batch_sample_indices):
        self.batch_id = batch_id
        self.last_accessed = None
        self.is_cached = False
        self.batch_sample_indices = batch_sample_indices
        self.next_access_time = float('inf')
        self.caching_in_progress = False
        self.lock = threading.Lock()
        self.predicted_access_times: dict = {}
        self.actual_access_times: dict = {}

    def set_caching_in_progress(self, in_progress: bool = False):
        with self.lock:
            self.caching_in_progress = in_progress

    def set_cached_status(self, is_cached: bool = False):
        with self.lock:
            self.is_cached = is_cached
            if is_cached:
                self.last_accessed = time.time()
    
    def set_predicted_access_time(self, job_id, predicted_time):
        self.predicted_access_times[job_id] = predicted_time

    
    def update_next_access_time(self, predicted_time, next_access_duration=60 * 60):
        # Update next access time if the predicted time is earlier than the current next access time
        with self.lock:
            if predicted_time < self.next_access_time:
                self.next_access_time = predicted_time + next_access_duration
            return self.next_access_time