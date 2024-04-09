import random
from queue import Queue
import copy
import time
import threading
from typing import List, Dict
from queue import PriorityQueue
from numpy import Infinity
import logging

SEED = 40
TOTAL_EPOCHS = 2
BATCHES_PER_EPOCH = 390
JOB_SPEEDS = [1]
CACHE_TTL = 5
PREFETCH_LOOKAHEAD = 5
KEEP_ALIVE_INTERVAL = 4

def configure_logger():
    logging.basicConfig(level=logging.INFO, format='%(message)s')
    logger = logging.getLogger("simulation")
    return logger

logger = configure_logger()


class Cache:
    def __init__(self, ttl, max_cache_size=None):
        self.cache = {}
        self.ttl = ttl
        self.lock = threading.Lock()  # Lock for thread safety
        self.expiration_times = {}
        # self.max_cache_size = max_cache_size

    def get(self, key):
        with self.lock:
            current_time = time.time()
            if key in self.cache and current_time < self.expiration_times[key]:
                return self.cache[key]
            else:
                self.remove(key)  # Remove expired key
                return None

    def set(self, key, value):
        with self.lock:
            current_time = time.time()
            self.cache[key] = value
            self.expiration_times[key] = current_time + self.ttl

    def remove(self, key):
        # with self.lock:
            if key in self.cache:
                del self.cache[key]
                del self.expiration_times[key]

    def __len__(self):
        with self.lock:
            return len(self.cache)

class TokenBucket:
    def __init__(self, capacity):
        self.capacity = capacity
        self.tokens = capacity
        # self.refill_rate = refill_rate
        self.last_refill_time = time.time()
        self.prefetched_batches = set()  # Keep track of downloaded/prefecthed bacthes
        self.lock = threading.Lock()  # Lock for accessing shared resources
    
    def refill(self, tokens_to_add =1):
        with self.lock:
            now = time.time()
            delta_time = now - self.last_refill_time
            # tokens_to_add = delta_time * self.refill_rate
            self.tokens = min(self.capacity, self.tokens + tokens_to_add)
            self.last_refill_time = now

    def consume(self, tokens):
        with self.lock:
            if tokens <= self.tokens:
                self.tokens -= tokens
                return True
            else:
                return False

    def wait_for_tokens(self):
        while not self.consume(1):
            time.sleep(0.1)


class MLJob:
    def __init__(self, id, speed, max_epochs, cache):
        self.job_id = id
        self.speed = speed
        self.current_epoch:Epoch = None
        self.cache:Cache = cache
        self.max_epochs = max_epochs
        self.epochs_processed = 0
        self.cache_hits = 0
        self.cache_misses = 0
    
    @property
    def cache_hit_ratio(self):
            if self.cache_hits + self.cache_misses == 0:
                return 0
            return (self.cache_hits / (self.cache_hits + self.cache_misses)) * 100

class Batch:
    def __init__(self, batch_id):
        self.batch_id = batch_id
        self.last_accessed_time = None
        self.lock = threading.Lock()  # Lock
        self.has_been_acessed_before = False
    
    def is_first_access(self):
        with self.lock:
            if self.has_been_acessed_before:
                return False
            else:
                self.has_been_acessed_before = True
                return True


    def reset_last_accessed(self):
        with self.lock:
            self.last_accessed_time = time.time()

class Epoch:
    def __init__(self, epoch_id:int, batches):
        self.epoch_id:int = epoch_id
        self.batches: Dict[str, Batch] = batches
        self.is_active = True
        self.pending_batch_accesses:Dict[int, List[Batch]] = {} #job id and batch_ids queue for that job
        self.lock = threading.Lock()
    
    def queue_up_batches_for_job(self, job_id):
        # Acquire lock to prevent modifications to self.batches
        with self.lock:
            if job_id not in self.pending_batch_accesses:
                self.pending_batch_accesses[job_id] = []
            for batch_id, batch in self.batches.items(): 
                self.pending_batch_accesses[job_id].append(batch)
    

class Simulator():
    def __init__(self, cache:Cache, epochs, token_bucket, jobs,keep_alive_interval):
        self.cache:Cache = cache
        self.epochs:List[Epoch] = epochs
        self.token_bucket:TokenBucket = token_bucket
        self.jobs:List[MLJob] = jobs
        self.cached_batches:List[Batch] = []
        self.keep_alive_interval =keep_alive_interval
        self.keep_alive_threshold = cache.ttl
        self.keep_alive_queue:PriorityQueue = PriorityQueue()
        self.active_epoch = None

    def prefetch(self):
        for epoch in self.epochs:
            self.active_epoch = epoch
            for batch_id, batch in epoch.batches.items():
                self.token_bucket.wait_for_tokens()
                self.cache.set(batch_id,batch_id)
                logger.info(f"Prefetched Epoch: {epoch.epoch_id},  BatchId: {batch_id}")

    

    def run_ml_job(self, job:MLJob):       
        while job.epochs_processed < job.max_epochs:
            time.sleep(job.speed)
            
            if job.current_epoch is None:
                epoch:Epoch = self.active_epoch
                job.current_epoch = self.active_epoch
                epoch.queue_up_batches_for_job(job.job_id)
            
            next_batch:Batch = job.current_epoch.pending_batch_accesses[job.job_id].pop(0)
            cache_hit = False
            if self.cache.get(next_batch.batch_id):
                cache_hit = True
                job.cache_hits +=1
            else:
                job.cache_misses +=1

            self.token_bucket.refill(1) if next_batch.is_first_access() else None
            
            logger.info(f'Job: {job.job_id}, Epoch: {epoch.epoch_id}, Batch: {next_batch.batch_id}, Cache Hit: {cache_hit}, Rate {job.cache_hit_ratio:.0f} %' )  

            if len(job.current_epoch.pending_batch_accesses[job.job_id]) < 1:
                job.epochs_processed +=1
                epoch:Epoch = self.active_epoch
                job.current_epoch = epoch
                epoch.queue_up_batches_for_job(job.job_id)


    def prefetch_worker(self):
        pass

    def keep_alive_worker(self):
        while True:
            for batch in self.cached_batches:
                time_since_last_accessed = batch.last_accessed_time - time.time()

                if time_since_last_accessed < self.evicition_timout:
                    next_access_time = Infinity
                    for job in self.jobs:
                        try:
                            next_access_time = min(job.speed * job.pending_batches.index(batch) , next_access_time)
                        except ValueError:
                            pass
                    self.keep_alive_queue.put(next_access_time, batch)
    
    def start(self):
        #start prefetching bacthes
        threads:List[threading.Thread] = []
        
        prefetch_thread = threading.Thread(target=self.prefetch, args=(), name='prefetcher')
        prefetch_thread.start()
        threads.append(prefetch_thread)

        #start jobs
        for job in self.jobs:
            ml_job_thread = threading.Thread(target=self.run_ml_job, args=(job,), name=f'job_{job.job_id}_thread')
            ml_job_thread.start()
            threads.append(ml_job_thread)
        
        # Join all threads
        for thread in threads:
            thread.join()



if __name__ == "__main__":
    random.seed(SEED)
    new_cache = Cache(CACHE_TTL)

    epochs:List[Epoch] = []
    for i in range(1, TOTAL_EPOCHS + 1):
        batches:Dict[str, Batch] = {}
        for y in range(BATCHES_PER_EPOCH):
            batch_id = f'{y+1}'
            new_batch = Batch(batch_id=batch_id)
            batches[batch_id] = new_batch
        new_epoch = Epoch(i, batches=batches)
        epochs.append(new_epoch)
    
    new_token_bucket = TokenBucket(capacity=PREFETCH_LOOKAHEAD)

    jobs:List[MLJob] =  []
    for idx, speed in enumerate(JOB_SPEEDS):
        new_job = MLJob(idx+1, speed, TOTAL_EPOCHS, new_cache)
        jobs.append(new_job)

    new_simulation = Simulator(
        cache=new_cache,
        epochs=epochs,
        token_bucket=new_token_bucket,
        jobs=jobs,
        keep_alive_interval=KEEP_ALIVE_INTERVAL,
    )

    new_simulation.start()