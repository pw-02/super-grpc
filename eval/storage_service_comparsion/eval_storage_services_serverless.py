import argparse
import time
import random
import pandas as pd
import os
import concurrent.futures
import boto3
from urllib.parse import urlparse
import json
from typing import List
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

def serverless_fetch(key):
    redis_host = '10.0.21.21'
    redis_port = 6378
    redis_client = redis.StrictRedis(host=redis_host, port=redis_port)
    return redis_client.get(key)




def read_remote_data_serverless(max_size_mb, file_keys:List[str], max_threads =1, shuffle=False,  print_freq=10):
    
    # print(f" Started Processing. max_size_mb: {max_size_mb}, max_threads: {max_threads}, shuffle: {shuffle}")
    # # Redis connection
    # redis_host = 't.rdior4.ng.0001.usw2.cache.amazonaws.com'
    # redis_port = 6379
    # redis_db = 0
    # redis_client = redis.StrictRedis(host=redis_host, port=redis_port, db=redis_db)

    #Get all keys
    print(f'num keys = {len(file_keys)}')

    if shuffle:
          random.seed(max_threads)
          random.shuffle(file_keys)
    
    files_to_download = file_keys
    total_size_loaded_mb = 0
    total_objects_loaded = 0

    start_time = time.perf_counter()
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_threads) as executor:
        futures = {executor.submit(serverless_fetch, s3_object_path): s3_object_path for s3_object_path in files_to_download}
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
     

def run_exp(process_id :int, file_lists:List[str], threads: List[int], max_size_mb:List[int], print_freq:int = 50, shuffle:bool = False,  num_processes:int =1):
    recorder = BenchmarkRecorder(['service', 'process_id', 'thread_count', 'total_objects', 'total_size (mb)', 'elapsed_time (s)', 'throughput (objects/s)', 'bandwidth (mb/s)'])
    local_directory = f'output'
    os.makedirs(local_directory, exist_ok=True)
    
    for size_mb in max_size_mb:
        for file_path in file_lists:
            keys = []
            with open(file_path, 'r') as f:
                keys = json.load(f)



            for thread_count in threads:    
                objects_count, total_size_mb, elapsed_time, throughput, bandwidth = read_remote_data_serverless(
                    max_size_mb=size_mb,
                    file_keys=keys,
                    max_threads = thread_count,
                    shuffle=shuffle,
                    print_freq=print_freq)
    
                
                recorder.add_record(
                                  {'service': 'serverless', 
                                #    'service': 's3', 
                                   'process_id':process_id,
                                   'thread_count': thread_count,
                                   'total_objects': objects_count, 
                                   'total_size (mb)':  total_size_mb,
                                   'elapsed_time (s)': elapsed_time,
                                   'throughput (objects/s)': throughput,
                                   'bandwidth (mb/s)': bandwidth}
                                   )
                #recorder.write(f'{local_directory}/{process_id}_redis_results.tsv')
                recorder.write(f'{local_directory}/serverless_results.tsv')



def parse_arguments():



    
    parser = argparse.ArgumentParser(description="Your script description here")
    parser.add_argument("--threads", type=int, nargs='+', default=[1, 2, 4, 8, 16, 32, 48, 72, 96, 128,196,212,256, 312,356,400], help="Max Threads")
    # parser.add_argument("--num_processes", type=int, nargs='+', default=1, help="Number of Processes")
    parser.add_argument("--max_size_mb", type=int, default=[3072], help="max_size_mb")
    parser.add_argument("--shuffle", action="store_true", help="Shuffle flag", default=True)
    parser.add_argument("--s3_dirs", type=list, default=[])
    # parser.add_argument("--redis_host", type=str, default=None, help="Redis host")
    # parser.add_argument("--serverless_host", type=str, default=None, help="Serverless host")
    # parser.add_argument("--throttling-mode", action="store_true", help="Shuffle flag")
    parser.add_argument("--print_freq", type=int, default=500, help="Print frequency")
    return parser.parse_args()

if __name__ == "__main__":
    
    args = parse_arguments()

    folder_path = "/home/ubuntu/filelists/"
    
    file_paths = ['/home/ubuntu/filelists/1mb_file_keys.json',
                  '/home/ubuntu/filelists/4mb_file_keys.json',
                  '/home/ubuntu/filelists/8mb_file_keys.json',
                  '/home/ubuntu/filelists/16mb_file_keys.json',
                  '/home/ubuntu/filelists/32mb_file_keys.json',
                  '/home/ubuntu/filelists/64mb_file_keys.json']
    # for root, dirs, files in os.walk(folder_path):
    #     for file in files:
    #         file_paths.append(os.path.join(root, file))
    

    
    run_exp(process_id=os.getpid(),
            file_lists=file_paths,
            threads=args.threads,
            max_size_mb=args.max_size_mb,
            print_freq=args.print_freq,
            shuffle= args.shuffle,
            num_processes=1)
    


    
    # # run(os.getpid(),parse_arguments(),s3_dirs)

    # for num_processes in args.processes:
    #       # Number of processes you want to spawn
    #       processes = []
    #       # Kick off multiple processes
    #       for i in range(num_processes):
    #            process = multiprocessing.Process(target=run, args=(i,parse_arguments(),s3_dirs,num_processes))
    #            processes.append(process)
    #            process.start()
          
    #       # Wait for all processes to finish
    #       for process in processes:
    #            process.join()
     
    #       print(f'All processes have finished for {num_processes}')



#     run(args = parse_arguments())