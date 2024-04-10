import random
from queue import Queue
import copy
import time
import threading
from typing import List, Dict
from queue import PriorityQueue
from numpy import Infinity
import logging
import os

SEED = 40
TOTAL_EPOCHS = 3
BATCHES_PER_EPOCH = 3906
JOB_SPEEDS = [0.1, 0.25]
CACHE_TTL = 600
PREFETCH_LOOKAHEAD = 50
KEEP_ALIVE_INTERVAL = 60
KEEP_ALIVE_THRESHOLD = 300 #5min since ;ast accessed

def format_timestamp(current_timestamp, use_utc=True):
    if use_utc:
        time_struct = time.gmtime(current_timestamp)
    else:
        time_struct = time.localtime(current_timestamp)

    formatted_current_time = time.strftime("%Y-%m-%d %H:%M:%S", time_struct)
    return formatted_current_time

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
            
    def update_last_accessed(self):
        with self.lock:
            self.last_accessed_time = time.time()

class Epoch:
    def __init__(self, epoch_id:int, batches):
        self.epoch_id:int = epoch_id
        self.batches: Dict[str, Batch] = batches
        self.is_active = True
        self.pending_batch_accesses:Dict[int, List[str]] = {} #job id and batch_ids queue for that job
        self.lock = threading.Lock()
    
    def queue_up_batches_for_job(self, job_id):
        # Acquire lock to prevent modifications to self.batches
        with self.lock:
            if job_id not in self.pending_batch_accesses:
                self.pending_batch_accesses[job_id] = list(self.batches.keys())
            # for batch_id, batch in self.batches.items(): 
            #     self.pending_batch_accesses[job_id] = 
    

class Simulator():
    def __init__(self, cache:Cache, epochs, token_bucket, jobs,keep_alive_interval, keep_alive_threshold):
        self.sim_id = os.getpid()
        self.cache:Cache = cache
        self.epochs:Dict[str,Epoch] = epochs
        self.token_bucket:TokenBucket = token_bucket
        self.jobs:Dict[str,MLJob] = jobs
        self.cached_batches:List[Batch] = []
        self.keep_alive_interval =keep_alive_interval
        self.keep_alive_threshold = keep_alive_threshold
        self.keep_alive_queue:PriorityQueue = PriorityQueue()
        self.active_epoch = None
        self.simulaton_ended = False

    def prefetch(self):
        for epoch_id, epoch in self.epochs.items():
            self.active_epoch = epoch
            for batch_id, batch in epoch.batches.items():
                self.token_bucket.wait_for_tokens()
                self.cache.set(batch_id,batch_id)
                batch.update_last_accessed()
                # logger.info(f"[{format_timestamp(batch.last_accessed_time)}]\t[Prefetched]\t[{batch_id}]")


    def run_ml_job(self, job:MLJob):       
        while job.epochs_processed < job.max_epochs: 
            if job.current_epoch is None:
                epoch:Epoch = self.active_epoch
                job.current_epoch = self.active_epoch
                epoch.queue_up_batches_for_job(job.job_id)
            
            time.sleep(job.speed)
            next_batch_id = job.current_epoch.pending_batch_accesses[job.job_id].pop(0)
            next_batch:Batch = job.current_epoch.batches[next_batch_id]
            # next_batch:Batch = job.current_epoch.pending_batch_accesses[job.job_id].pop(0)
            cache_hit_message = 'miss'
            if self.cache.get(next_batch.batch_id):
                job.cache_hits +=1
                next_batch.update_last_accessed()
                cache_hit_message = 'hit'
            else:
                job.cache_misses +=1

            self.token_bucket.refill(1) if next_batch.is_first_access() else None
            
            logger.info(f'[{format_timestamp(next_batch.last_accessed_time)}]\t[Processed]\t[job_{job.job_id}, {next_batch.batch_id}, {cache_hit_message}, {job.cache_hit_ratio:.0f}%]'  )  

            if len(job.current_epoch.pending_batch_accesses[job.job_id]) < 1:
                job.epochs_processed +=1
                epoch:Epoch = self.active_epoch
                job.current_epoch = epoch
                epoch.queue_up_batches_for_job(job.job_id)
        
        end_simualtion = True
        for job_id, job in self.jobs.items():
            if job.epochs_processed < job.max_epochs:
                end_simualtion = False
                break
        self.simulaton_ended = end_simualtion



    def prefetch_worker(self):
        pass

    def epoch_is_active(self, epoch_id):
        if self.active_epoch.epoch_id == epoch_id:
            return True
        is_active = False
        for job_id, job in self.jobs.items():
            if  job.current_epoch is not None and job.current_epoch.epoch_id == epoch_id:
                is_active = True
                break
        return is_active
    
    def get_batch_next_access_time(self, epoch_id, batch_id):
        next_access_time = float('inf')  
        for job_id in self.epochs[epoch_id].pending_batch_accesses:
            try:
                next_access_time = min(
                    time.time() + self.jobs[job_id].speed * self.epochs[epoch_id].pending_batch_accesses[job_id].index(batch_id), next_access_time)
            except ValueError:
                pass
        return next_access_time

    def keep_alive_producer(self):
        while not self.simulaton_ended:
            time.sleep(self.keep_alive_interval)
            for epoch_id, epoch in self.epochs.items():
                if self.epoch_is_active(epoch.epoch_id):
                    for bacth_id, batch in epoch.batches.items():
                        if batch.last_accessed_time is not None:
                            time_since_last_accessed = time.time() - batch.last_accessed_time
                            if time_since_last_accessed > self.keep_alive_threshold:
                                next_access_time = self.get_batch_next_access_time(epoch_id, batch.batch_id)
                                if next_access_time != float('inf'):
                                    self.keep_alive_queue.put((next_access_time, batch))
                                    logger.info(f'[{format_timestamp(time.time())}]\t[KeepAlive]\t[{batch.batch_id}]'  )  

                                # else:
                                #     logger.info(f"No next access time found for batch {batch.batch_id} in epoch {epoch_id}")
            
    
    def keep_alive_consumer(self):
        while not self.simulaton_ended:
            if self.keep_alive_queue.empty():
                time.sleep(0.1)
            else:
                item = self.keep_alive_queue.get()
                next_batch:Batch = item[1]
                self.cache.set(next_batch.batch_id,next_batch.batch_id)
                next_batch.update_last_accessed()
    def start(self):
        #start prefetching bacthes
        threads:List[threading.Thread] = []
        
        prefetch_thread = threading.Thread(target=self.prefetch, args=(), name='prefetch_thread')
        prefetch_thread.start()
        threads.append(prefetch_thread)
        
        time.sleep(0.1)
        #start jobs
        for job_id, job in self.jobs.items():
            ml_job_thread = threading.Thread(target=self.run_ml_job, args=(job,), name=f'job_{job.job_id}_thread')
            ml_job_thread.start()
            threads.append(ml_job_thread)

        #start keep alive thread 
        keep_alive_producer = threading.Thread(target=self.keep_alive_producer, args=(), name='keep_alive_producer')
        keep_alive_producer.start()
        threads.append(keep_alive_producer)

        #start keep alive thread 
        keep_alive_consumer = threading.Thread(target=self.keep_alive_consumer, args=(), name='keep_alive_consumer_thread')
        keep_alive_consumer.start()
        threads.append(keep_alive_consumer)

        # Join all threads
        for thread in threads:
            thread.join()

    def report_results(self, filename):
        import csv
        report_lines = []
        for job_id, job in self.jobs.items():
            report_lines.append(
                {   'SimualtionId': self.sim_id,
                    'CacheTTL(s)': CACHE_TTL,
                    'KeepAliveInterval':KEEP_ALIVE_INTERVAL,
                    'KeepAliveThreshold': KEEP_ALIVE_THRESHOLD,
                    'PreftechLookahead': PREFETCH_LOOKAHEAD,
                    'JobId': job.job_id,
                    'JobSpeed': job.speed,
                    'Epochs': job.max_epochs,
                    'NumBatches': job.cache_hits+job.cache_misses,
                    'Hits': job.cache_hits,
                    'Misses': job.cache_misses,
                    'Rate': job.cache_hit_ratio
                    })
    
        # Extract the keys from the dictionary
        fieldnames = report_lines[0].keys() if report_lines else []
    # Write dictionary to CSV
        with open(filename, 'a+', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            
            # Check if the file is empty (i.e., it's newly created)
            csvfile.seek(0)
            first_char = csvfile.read(1)
            csvfile.seek(0)
            
            # Write header only if the file is newly created
            if not first_char:
                writer.writeheader()

            # Write rows
            for row in report_lines:
                writer.writerow(row)


if __name__ == "__main__":
    random.seed(SEED)
    new_cache = Cache(CACHE_TTL)

    epochs:Dict[str,Epoch] = {}
    for i in range(1, TOTAL_EPOCHS + 1):
        batches:Dict[str, Batch] = {}
        for y in range(BATCHES_PER_EPOCH):
            batch_id = f'epoch_{i}_batch_{y+1}'
            new_batch = Batch(batch_id=batch_id)
            batches[batch_id] = new_batch
        new_epoch = Epoch(i, batches=batches)
        epochs[i] = new_epoch
    
    new_token_bucket = TokenBucket(capacity=PREFETCH_LOOKAHEAD)

    jobs:Dict[str,MLJob] =  {}
    for idx, speed in enumerate(JOB_SPEEDS):
        new_job = MLJob(idx+1, speed, TOTAL_EPOCHS, new_cache)
        jobs[new_job.job_id] = new_job

    new_simulation = Simulator(
        cache=new_cache,
        epochs=epochs,
        token_bucket=new_token_bucket,
        jobs=jobs,
        keep_alive_interval=KEEP_ALIVE_INTERVAL,
        keep_alive_threshold=KEEP_ALIVE_THRESHOLD
    )

    new_simulation.start()
    new_simulation.report_results('simulation/sim_output.csv')