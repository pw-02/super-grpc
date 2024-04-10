import simpy
import random
import time
from typing import List, Dict
import os

SEED = 40
TOTAL_EPOCHS = 3
BATCHES_PER_EPOCH = 100
JOB_SPEEDS = [2,1]
CACHE_TTL = 600
PREFETCH_LOOKAHEAD = 50
KEEP_ALIVE_INTERVAL = 60
KEEP_ALIVE_THRESHOLD = 300  # 5 min since last accessed
PREFETCH_RATE = min(JOB_SPEEDS)

class Cache:
    def __init__(self, ttl, max_cache_size=None):
        self.cache = {}
        self.ttl = ttl

    def set(self, key, env:simpy.Environment):
        expiration_time = env.now + self.ttl
        self.cache[key] = expiration_time
    
    def get(self, key, env:simpy.Environment):
        if key in self.cache and self.cache[key] > env.now:
            return True
        else:
            self.remove(key)  # Remove expired key
            return False
         
    def remove(self, key):
        if key in self.cache:
            del self.cache[key]

    def __len__(self):
            return len(self.cache)

class MLJob:
    def __init__(self, id, speed, max_epochs, cache):
        self.job_id = id
        self.speed = speed
        self.current_epoch = None
        self.cache = cache
        self.max_epochs = max_epochs
        self.epochs_processed = 0
        self.cache_hits = 0
        self.cache_misses = 0
        self.job_ened = False

    @property
    def cache_hit_ratio(self):
        if self.cache_hits + self.cache_misses == 0:
            return 0
        return (self.cache_hits / (self.cache_hits + self.cache_misses)) * 100
    
class Batch:
    def __init__(self, batch_id):
        self.batch_id = batch_id
        self.last_accessed_time = None
        self.has_been_acessed_before = False

    def is_first_access(self):
            if self.has_been_acessed_before:
                return False
            else:
                self.has_been_acessed_before = True
                return True
            
    def update_last_accessed(self):
            self.last_accessed_time = time.time()

class Epoch:
    def __init__(self, epoch_id: int, num_batches:int):
        self.epoch_id = epoch_id
        self.batches: Dict[str, Batch] ={}
        for i in range(1, num_batches+1):
            batch_id=f'batch_{epoch_id}_{i}'
            self.batches[batch_id] = Batch(batch_id=batch_id)
        self.is_active = True
        self.pending_batch_accesses: Dict[int, List[str]] = {}  # job id and batch_ids queue for that job

    def queue_up_batches_for_job(self, job_id):
        if job_id not in self.pending_batch_accesses:
            self.pending_batch_accesses[job_id] = list(self.batches.keys())


class SimpyWrapper:
    def __init__(self, cache: Cache, jobs, keep_alive_interval, keep_alive_threshold, lookahead, preftech_rate):
        self.cache = cache
        self.epochs:Dict[str, Epoch] = {}
        self.jobs:Dict[str, MLJob] = jobs
        self.keep_alive_interval = keep_alive_interval
        self.keep_alive_threshold = keep_alive_threshold
        self.active_epoch:Epoch = None
        self.lookahead_distance= lookahead
        self.preftech_rate = preftech_rate
        self.env = simpy.Environment()
        self.end_simulation = False
        self.epoch_counter = 1

    def prefetch(self):
        is_first_epoch = True
        while not self.end_simulation:
            next_epoch = Epoch(self.epoch_counter, BATCHES_PER_EPOCH)
            self.active_epoch = next_epoch
            self.epochs[next_epoch.epoch_id] = next_epoch
            epoch_batches:List[Batch] = list(self.active_epoch.batches.values())

            if is_first_epoch:
                for batch in epoch_batches[:self.lookahead_distance]:
                    self.cache.set(batch.batch_id, self.env)
                    print(f"Prefecther added {batch.batch_id} to cache at time {self.env.now}s")
                    batch.update_last_accessed()
                yield self.env.timeout(0)

                for batch in epoch_batches[self.lookahead_distance:]:
                    yield self.env.timeout(self.preftech_rate)
                    self.cache.set(batch.batch_id,self.env)
                    print(f"Prefecther added {batch.batch_id} to cache at time {self.env.now}s")
                    batch.update_last_accessed()
                is_first_epoch = False
            else:
                for batch in epoch_batches:

                    yield self.env.timeout(self.preftech_rate)
                    if self.end_simulation:
                        break
                    self.cache.set(batch.batch_id,self.env)
                    print(f"Prefecther added {batch.batch_id} to cache at time {self.env.now}s")
                    batch.update_last_accessed()
                

            self.epoch_counter+=1

    def run_ml_job(self, job: MLJob):  
        for idx in range(job.max_epochs):
            job.current_epoch = self.active_epoch
            job.current_epoch.queue_up_batches_for_job(job.job_id)
            
            while len(job.current_epoch.pending_batch_accesses[job.job_id]) > 0:
                yield self.env.timeout(job.speed)

                next_batch_id = job.current_epoch.pending_batch_accesses[job.job_id].pop(0)
                next_batch: Batch = job.current_epoch.batches[next_batch_id]

                if self.cache.get(next_batch.batch_id,self.env):
                    job.cache_hits += 1
                    next_batch.update_last_accessed()
                    print(f"Job {job.job_id} hit {next_batch_id} in cache at time {self.env.now}s")
                else:
                    job.cache_misses += 1
                    print(f"Job {job.job_id} missed {next_batch_id} in cache at time {self.env.now}s")
            
        job.job_ened = True
        end_simualtion = True
        fastest_job_rate = float('inf')
        for job in self.jobs.values():
            if job.job_ened == False:
                fastest_job_rate = min(fastest_job_rate, job.speed)
                end_simualtion = False
        
        if not end_simualtion:
            self.preftech_rate = fastest_job_rate
        self.end_simulation = end_simualtion

 
    # def epoch_is_active(self, epoch_id):
    #     if self.active_epoch.epoch_id == epoch_id:
    #         return True
    #     is_active = False
    #     for job_id, job in self.jobs.items():
    #         if  job.current_epoch is not None and job.current_epoch.epoch_id == epoch_id:
    #             is_active = True
    #             break
    #     return is_active
    
    # def get_batch_next_access_time(self, epoch_id, batch_id):
    #     next_access_time = float('inf')  
    #     for job_id in self.epochs[epoch_id].pending_batch_accesses:
    #         try:
    #             next_access_time = min(
    #                 time.time() + self.jobs[job_id].speed * self.epochs[epoch_id].pending_batch_accesses[job_id].index(batch_id), next_access_time)
    #         except ValueError:
    #             pass
    #     return next_access_time
    
    # def keep_alive_producer(self):
    #    while not self.simulaton_ended:
    #         yield self.env.timeout(self.keep_alive_interval)
    #         for epoch_id, epoch in self.epochs.items():
    #             if self.epoch_is_active(epoch.epoch_id):
    #                 for batch_id, batch in epoch.batches.items():
    #                     if batch.last_accessed_time is not None:
    #                         time_since_last_accessed = time.time() - batch.last_accessed_time
    #                         if time_since_last_accessed > self.keep_alive_threshold:
    #                             next_access_time = self.get_batch_next_access_time(epoch_id, batch.batch_id)
    #                             if next_access_time != float('inf'):
    #                                 self.cache.set(batch.batch_id,self.env)
    #                                 batch.update_last_accessed()

    def start(self):
        self.env.process(self.prefetch())
        for job_id, job in self.jobs.items():
            self.env.process(self.run_ml_job(job))
        # self.env.process(self.keep_alive_producer())
        # self.env.process(self.keep_alive_consumer())
        self.env.run()

    def report_results(self, filename):
        import csv
        report_lines = []
        for job_id, job in self.jobs.items():
            report_lines.append(
                {'SimualtionId': os.getpid(),
                 'CacheTTL(s)': CACHE_TTL,
                 'KeepAliveInterval': KEEP_ALIVE_INTERVAL,
                 'KeepAliveThreshold': KEEP_ALIVE_THRESHOLD,
                 'PreftechLookahead': PREFETCH_LOOKAHEAD,
                 'JobId': job.job_id,
                 'JobSpeed': job.speed,
                 'Epochs': job.max_epochs,
                 'NumBatches': job.cache_hits + job.cache_misses,
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

    jobs: Dict[str, MLJob] = {}
    for idx, speed in enumerate(JOB_SPEEDS):
        new_job = MLJob(idx + 1, speed, TOTAL_EPOCHS, new_cache)
        jobs[new_job.job_id] = new_job

    simpy_wrapper = SimpyWrapper(
        cache=new_cache,
        jobs=jobs,
        keep_alive_interval=KEEP_ALIVE_INTERVAL,
        keep_alive_threshold=KEEP_ALIVE_THRESHOLD,
        lookahead=PREFETCH_LOOKAHEAD,
        preftech_rate = PREFETCH_RATE
    )

    simpy_wrapper.start()
    simpy_wrapper.report_results('simulation/sim_output.csv')
