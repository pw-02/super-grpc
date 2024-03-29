import argparse
import time
import random
import pandas as pd
import os
import concurrent.futures
import boto3
from urllib.parse import urlparse
import json
import multiprocessing
from typing import List

class S3Url(object):
    def __init__(self, url):
        self._parsed = urlparse(url, allow_fragments=False)

    @property
    def bucket(self):
        return self._parsed.netloc

    @property
    def key(self):
        if self._parsed.query:
            return self._parsed.path.lstrip('/') + '?' + self._parsed.query
        else:
            return self._parsed.path.lstrip('/')

    @property
    def url(self):
        return self._parsed.geturl()


class S3Helper():
    def __init__(self):
        self.s3_client = boto3.client('s3')
    
    def check_file_exits(self, file_path):
        try:
            self.s3_client.head_object(Bucket=S3Url(file_path).bucket, Key=S3Url(file_path).key)
            return True
        except Exception:
            return False
        
    def get_bucket_name(self, url):
        return S3Url(url).bucket
    
    def load_object_paths(self, data_dir, isImages = True, create_index = True):
        classed_samples = {}
        s3url = S3Url(data_dir)
        s3_resource = boto3.resource("s3")
        file_list = []
        try:
            index_object = s3_resource.Object(s3url.bucket, s3url.key + 'file_list.json')
            try:
                file_content = index_object.get()['Body'].read().decode('utf-8')
                file_list = json.loads(file_content)
            except:
        
                paginator = self.s3_client.get_paginator('list_objects_v2')
                pages = paginator.paginate(Bucket=s3url.bucket, Prefix=s3url.key)
                for page in pages:
                    for blob in page['Contents']:
                        blob_path = blob.get('Key')
                        blob_size =  blob.get('Size')
                        # Check if the object is a folder which we want to ignore
                        if blob_path[-1] == "/":
                            continue
                        stripped_path = remove_prefix(blob_path, s3url.key).lstrip("/")
                        #Indicates that it did not match the starting prefix
                        if stripped_path == blob_path:
                            continue
                        if isImages and not self.is_image_file(blob_path):
                            continue
                        file_list.append((blob_path, blob_size))          
            
                if create_index:
                    index_object = s3_resource.Object(s3url.bucket, s3url.key + 'file_list.json')
                    index_object.put(Body=(bytes(json.dumps(file_list, indent=4).encode('UTF-8'))))
            return file_list
        except Exception as e:
            print(f"Error listing files and subfolders in S3 subfolder: {str(e)}")
            return []
        

    def is_image_file(self, filename: str):
        return any(filename.endswith(extension) for extension in ['.jpg', '.JPG', '.jpeg', '.JPEG', '.png', '.PNG', '.ppm', '.PPM', '.bmp', '.BMP'])
    
    def read_s3_object(self, bucket, file_path, is_image=False):
        obj = self.s3_client.get_object(Bucket=bucket, Key=file_path)
        if is_image:
            content = obj['Body'].read()
        else:    
            content = obj['Body'].read().decode('utf-8')
        object_size_bytes = obj['ContentLength']
        return content, object_size_bytes
    
    def upload_to_s3(self, file_path, bucket_name, s3_prefix=''):
        # Create an S3 client
        s3 = boto3.client('s3')
        try:
            # Upload the file with the specified key (path) to the specified bucket
            s3.upload_file(file_path, bucket_name, s3_prefix)
            print(f"File '{file_path}' uploaded to S3 bucket '{bucket_name}'")
        except FileNotFoundError:
            print(f"The file '{file_path}' was not found.")
  


      
def list_files_and_subfolders(s3url, save_index=True):
    sample_paths = []
    s3_resource = boto3.resource("s3")
    s3_client = boto3.client('s3')

    try:
        index_object = s3_resource.Object(s3url.bucket, s3url.key + 'index.json')
        try:
            file_content = index_object.get()['Body'].read().decode('utf-8')
            sample_paths = json.loads(file_content)
        except:
        
            # s3url = S3Url(s3url)
            paginator = s3_client.get_paginator('list_objects_v2')
            pages = paginator.paginate(Bucket=s3url.bucket, Prefix=s3url.key)
            for page in pages:
                for blob in page['Contents']:
                    blob_path = blob.get('Key')
                    # Check if the object is a folder which we want to ignore
                    if blob_path[-1] == "/":
                        continue
                    stripped_path = remove_prefix(blob_path, s3url.key).lstrip("/")
                    # Indicates that it did not match the starting prefix
                    if stripped_path == blob_path:
                        continue
                    sample_paths.append(blob_path)
            
            if save_index:
                index_object = s3_resource.Object(s3url.bucket, s3url.key + 'index.json')
                index_object.put(Body=(bytes(json.dumps(sample_paths, indent=4).encode('UTF-8'))))
        return sample_paths
       

    except Exception as e:
        print(f"Error listing files and subfolders in S3 subfolder: {str(e)}")
        return []

def remove_prefix(s: str, prefix: str) -> str:
        if not s.startswith(prefix):
            return s
        return s[len(prefix) :]


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


def read_remote_data_s3(s3_dir, max_size_mb, max_threads =1, shuffle=False, print_freq=10):  
    print(f" Started Processing. max_size_mb: {max_size_mb}, max_threads: {max_threads}, shuffle: {shuffle}, s3_dir: {s3_dir}")
    s3_helper = S3Helper()
    bucket_name = s3_helper.get_bucket_name(s3_dir)
    file_list_with_sizes = s3_helper.load_object_paths(s3_dir, isImages=False, create_index=True)

    if shuffle:
        random.seed(max_threads)
        random.shuffle(file_list_with_sizes)
        
    # Number of items selected
    files_to_download = []
    total_size_mb = 0

    # Iterate over shuffled list until total size exceeds max_size_mb
    for file_path, file_size_bytes in file_list_with_sizes:
          total_size_mb += file_size_bytes / (1024 * 1024)  # Convert Bytes to MB
          files_to_download.append((file_path, file_size_bytes))
          if total_size_mb >= max_size_mb:
               break

    total_size_loaded_mb = 0
    total_objects_loaded = 0

    start_time = time.perf_counter() 
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_threads) as executor:
        futures = {executor.submit(s3_helper.read_s3_object, bucket_name, s3_object_path[0], True): s3_object_path for s3_object_path in files_to_download}
        # Iterate over the futures as they complete
        for future in concurrent.futures.as_completed(futures):
            key = futures[future]
            try:
                s3_object_content, object_size_bytes = future.result()
                object_size_mb  = object_size_bytes / (1024 * 1024)  # Convert Bytes to MB
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


def run_exp(process_id :int, s3_dirs:List[str], threads: List[int], max_size_mb:List[int], print_freq:int = 50, shuffle:bool = False,  num_processes:int =1):
    recorder = BenchmarkRecorder(['service', 'process_id', 'thread_count', 'total_objects', 'total_size (mb)', 'elapsed_time (s)', 'throughput (objects/s)', 'bandwidth (mb/s)'])
    local_directory = f'output/s3/{num_processes}_process'
    os.makedirs(local_directory, exist_ok=True)
    
    for size_mb in max_size_mb:
        for s3_dir in s3_dirs:
            for thread_count in threads:    
                objects_count, total_size_mb, elapsed_time, throughput, bandwidth = read_remote_data_s3(
                    s3_dir=s3_dir,
                    max_size_mb=size_mb,
                    max_threads = thread_count,
                    shuffle=shuffle,
                    print_freq=print_freq)
                
                recorder.add_record(
                                  {'service': 's3', 
                                #    'service': 's3', 
                                   'process_id':process_id,
                                   'thread_count': thread_count,
                                   'total_objects': objects_count, 
                                   'total_size (mb)':  total_size_mb,
                                   'elapsed_time (s)': elapsed_time,
                                   'throughput (objects/s)': throughput,
                                   'bandwidth (mb/s)': bandwidth}
                                   )
                recorder.write(f'{local_directory}/{process_id}_s3_results.tsv')

def parse_arguments():
    s3_dirs =['s3://storage.services.bench.data/0.5_10240/',
              's3://storage.services.bench.data/1_5120/',  
              's3://storage.services.bench.data/2_2560/',
              's3://storage.services.bench.data/4_1280/',
              's3://storage.services.bench.data/8_640/',
               's3://storage.services.bench.data/16_320/',
               's3://storage.services.bench.data/32_160/'
               ]
    
    parser = argparse.ArgumentParser(description="Your script description here")
    parser.add_argument("--threads", type=int, nargs='+', default=[1], help="Max Threads")
    # parser.add_argument("--num_processes", type=int, nargs='+', default=1, help="Number of Processes")
    parser.add_argument("--max_size_mb", type=int, default=[5], help="max_size_mb")
    parser.add_argument("--shuffle", action="store_true", help="Shuffle flag", default=True)
    parser.add_argument("--s3_dirs", type=list, default=s3_dirs)
    # parser.add_argument("--redis_host", type=str, default=None, help="Redis host")
    # parser.add_argument("--serverless_host", type=str, default=None, help="Serverless host")
    # parser.add_argument("--throttling-mode", action="store_true", help="Shuffle flag")
    parser.add_argument("--print_freq", type=int, default=500, help="Print frequency")
    return parser.parse_args()

if __name__ == "__main__":
    
    args = parse_arguments()
    
    run_exp(process_id=os.getpid(),
            s3_dirs=args.s3_dirs,
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