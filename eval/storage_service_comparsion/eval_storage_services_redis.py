import argparse
import time
import random
import pandas as pd
import os
import concurrent.futures
from urllib.parse import urlparse
import multiprocessing
import redis
import sys

class BenchmarkRecorder:
     def __init__(self, columns):
        self.df = pd.DataFrame(columns=columns)
        self.column_names = columns

     def add_record(self, new_record):
        self.df.loc[len(self.df)] = new_record
     
     def write(self, filename, append=True):
          mode = 'a' if append else 'w'
          header = not append or not os.path.exists(filename)  # Check if file doesn't exist or it's a new file
          self.df.to_csv(filename, sep='\t', index=False, mode=mode, header=header)
          self.df.drop(self.df.index, inplace=True)  # Clear the DataFrame after writing to file

def display_stats(objects_count,total_size_mb,elapsed_time):
     throughput = objects_count / elapsed_time
     bandwidth = total_size_mb / elapsed_time
     stats_str = f"Total Objects: {objects_count}, Total Size (Mb): {total_size_mb}, Time elapsed: {elapsed_time:.4f} seconds, Throughput: {throughput:.4f} objects/s, Bandwidth: {bandwidth:.4f} MB/s"
     print(stats_str)

def read_remote_data_redis(max_size_mb, max_threads =1, shuffle=False, seed_value=None, print_freq=10):

    print(f" Started Processing. max_size_mb: {max_size_mb}, max_threads: {max_threads}, shuffle: {shuffle}")
    # Redis connection
    redis_host = 't1.rdior4.ng.0001.usw2.cache.amazonaws.com'
    redis_port = 6379
    redis_db = 0
    redis_client = redis.StrictRedis(host=redis_host, port=redis_port, db=redis_db)
    file_list_with_sizes =[]
    # Get all keys
    all_keys = redis_client.keys('*')
    print(f'num keys = {len(all_keys)}')
    all_keys_decoded = [key.decode('utf-8') for key in all_keys]
        
    if shuffle:
          random.seed(max_threads)
          random.shuffle(all_keys_decoded)
    #Number of items selected
    x = 2048
    files_to_download = all_keys_decoded[:x]

    total_size_loaded_mb = 0
    total_objects_loaded = 0
    start_time = time.perf_counter()
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_threads) as executor:
          futures = {executor.submit(redis_client.get, s3_object_path): s3_object_path for s3_object_path in files_to_download}
          # Iterate over the futures as they complete
          for future in concurrent.futures.as_completed(futures):
               key = futures[future]
               try:
                        reids_object = future.result()
                        object_size_mb  = sys.getsizeof(reids_object) / (1024 * 1024)
                        total_size_loaded_mb += object_size_mb
                        total_objects_loaded += 1
                        if total_objects_loaded % print_freq == 0:  # Calculate metrics every x files
                              elapsed_time = time.perf_counter() - start_time               
                              display_stats(total_objects_loaded,total_size_loaded_mb,elapsed_time)        
               
               except Exception as e:
                    print(f"Error downloading {key}: {e}")
          
    elapsed_time = time.perf_counter() - start_time
    display_stats(total_objects_loaded,total_size_loaded_mb,elapsed_time)
    print(f"Reached end of file iist, Stopping.")
    return total_objects_loaded, total_size_loaded_mb, elapsed_time, total_objects_loaded / elapsed_time, total_size_loaded_mb / elapsed_time
     
def run(process_id, args, num_processes =1):
     recorder = BenchmarkRecorder(['service', 'process_id', 'thread_count', 'total_objects', 'total_size (mb)', 'elapsed_time (s)', 'throughput (objects/s)', 'bandwidth (mb/s)'])
     local_directory = f'output2/redis/processes_{num_processes}'
     os.makedirs(local_directory, exist_ok=True)
     for size_mb  in args.max_size_mb:
               for thread_count in args.threads:    
                    objects_count, total_size_mb, elapsed_time, throughput, bandwidth = read_remote_data_redis(
                              max_size_mb=size_mb,
                              max_threads = thread_count,
                              shuffle=args.shuffle,
                              seed_value=args.seed, 
                              print_freq=args.print_freq)

                    recorder.add_record({'service': 'redis', 
                                   'service': 'redis', 
                                   'process_id':process_id,
                                   'thread_count': thread_count,
                                   'total_objects': objects_count, 
                                   'total_size (mb)':  total_size_mb,
                                   'elapsed_time (s)': elapsed_time,
                                   'throughput (objects/s)': throughput,
                                   'bandwidth (mb/s)': bandwidth})
                    recorder.write(f'{local_directory}/{process_id}_redis_results.tsv')

def parse_arguments():
    parser = argparse.ArgumentParser(description="Your script description here")
    parser.add_argument("--threads", type=int, nargs='+', default=[32], help="List of threads per process")
    parser.add_argument("--processes", type=int, nargs='+', default=[1,2,4,8,16,32], help="List of threads per process")
    parser.add_argument("--max_size_mb", type=int, default=[1024], help="max_size_mb")
    parser.add_argument("--seed", type=int, default=41, help="Seed value")
    parser.add_argument("--s3_dir", type=str, default="s3://imagenet1k-sdl/train/", help="S3 bucket name")
    parser.add_argument("--redis_host", type=str, default=None, help="Redis host")
    parser.add_argument("--serverless_host", type=str, default=None, help="Serverless host")
    parser.add_argument("--shuffle", action="store_true", help="Shuffle flag", default=True)
    parser.add_argument("--print_freq", type=int, default=500000000, help="Print frequency")
    parser.add_argument("--throttling-mode", action="store_true", help="Shuffle flag")
    return parser.parse_args()

if __name__ == "__main__":

     args = parse_arguments()
     
     for num_processes in args.processes:
          # Number of processes you want to spawn
          processes = []
          # Kick off multiple processes
          for i in range(num_processes):
               process = multiprocessing.Process(target=run, args=(i,parse_arguments(),num_processes))
               processes.append(process)
               process.start()
          
          # Wait for all processes to finish
          for process in processes:
               process.join()
     
          print(f'All processes have finished for {num_processes}')



#     run(args = parse_arguments())