
import boto3
from urllib.parse import urlparse
import json
import time
from boto3.exceptions import botocore

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
    
def get_boto_s3_client():
    return boto3.client('s3')

def s3_object_exists(file_path:str, s3_client = None):
    try:
        if s3_client is None:
            s3_client = get_boto_s3_client()
            s3_client.head_object(Bucket=S3Url(file_path).bucket, Key=S3Url(file_path).key)
            return True
    except Exception:
        return False


def load_unpaired_s3_object_keys(s3_uri:str, images_only:bool, use_index_file:bool = True):
    sample_ulrs = []
    s3url = S3Url(s3_uri)
    s3_client = boto3.client('s3')
    index_file_key = s3url.key + '_unpaired_index.json'
    if use_index_file:
        try:
            index_object = s3_client.get_object(Bucket=s3url.bucket, Key=index_file_key)
            file_content = index_object['Body'].read().decode('utf-8')
            paired_samples = json.loads(file_content)
            return paired_samples
        except botocore.exceptions.ClientError as e:
            print(f"Error reading index file '{index_file_key}': {str(e)}")

    # If use_index_file is False or encounter errors with index file, build paired_samples from S3 objects
    paginator = s3_client.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=s3url.bucket, Prefix=s3url.key)
    for page in pages:
        for blob in page.get('Contents', []):
            blob_path = blob.get('Key')
            if blob_path.endswith("/"):
                continue  # Ignore folders
            
            stripped_path = remove_prefix(blob_path, s3url.key).lstrip("/")
            if stripped_path == blob_path:
                continue  # No matching prefix, skip
            
            if images_only and not is_image_file(blob_path):
                continue  # Skip non-image files
            
            sample_ulrs.append(blob_path)
              
    if use_index_file and len(sample_ulrs) > 0:
        index_object = s3_client.put_object(Bucket=s3url.bucket, Key=index_file_key, 
                                            Body=json.dumps(sample_ulrs, indent=4).encode('UTF-8'))    
    return sample_ulrs


def load_paired_s3_object_keys(s3_uri:str, images_only:bool, use_index_file:bool = True):
    paired_samples = {}
    s3url = S3Url(s3_uri)
    s3_client = boto3.client('s3')
    index_file_key = s3url.key + '_paired_index.json'
    if use_index_file:
        try:
            index_object = s3_client.get_object(Bucket=s3url.bucket, Key=index_file_key)
            file_content = index_object['Body'].read().decode('utf-8')
            paired_samples = json.loads(file_content)
            return paired_samples
        except botocore.exceptions.ClientError as e:
            print(f"Error reading index file '{index_file_key}': {str(e)}")

    # If use_index_file is False or encounter errors with index file, build paired_samples from S3 objects
    paginator = s3_client.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=s3url.bucket, Prefix=s3url.key)
    for page in pages:
        for blob in page.get('Contents', []):
            blob_path = blob.get('Key')
            if blob_path.endswith("/"):
                continue  # Ignore folders
            
            stripped_path = remove_prefix(blob_path, s3url.key).lstrip("/")
            if stripped_path == blob_path:
                continue  # No matching prefix, skip
            
            if images_only and not is_image_file(blob_path):
                continue  # Skip non-image files
            
            if 'index.json' in blob_path:
                continue

            blob_class = stripped_path.split("/")[0]
            blobs_with_class = paired_samples.get(blob_class, [])
            blobs_with_class.append(blob_path)
            paired_samples[blob_class] = blobs_with_class
    
    if use_index_file and len(paired_samples) > 0:
        index_object = s3_client.put_object(Bucket=s3url.bucket, Key=index_file_key, 
                                            Body=json.dumps(paired_samples, indent=4).encode('UTF-8'))    
    return paired_samples


 
def is_image_file(path: str):
    return any(path.endswith(extension) for extension in ['.jpg', '.JPG', '.jpeg', '.JPEG', '.png', '.PNG', '.ppm', '.PPM', '.bmp', '.BMP'])
    
def get_s3_object(bucket, object_uri:str, s3_client = None):  
    if s3_client is None:
            s3_client = get_boto_s3_client()
            
    obj = s3_client.get_object(Bucket=bucket, Key=object_uri)
    if is_image_file(object_uri):
         content = obj['Body'].read()
    else:
        content = obj['Body'].read().decode('utf-8')
    return content


def remove_prefix(s: str, prefix: str) -> str:
        if not s.startswith(prefix):
            return s
        return s[len(prefix) :]

class AWSLambdaClient():

    def __init__(self):
        self.lambda_client = None
    
    def invoke_function(self, function_name:str, payload, simulate = False):
        start_time = time.perf_counter()
        if simulate:
            time.sleep(0.01)
            response ={}
            response['duration'] = time.perf_counter() - start_time
            response['message'] = ""
            response['success'] = True
        else:
            if  self.lambda_client is None:
                self.lambda_client = boto3.client('lambda') 

            response = self.lambda_client.invoke(FunctionName=function_name,InvocationType='RequestResponse',Payload=payload)
            response_data = json.loads(response['Payload'].read().decode('utf-8'))
            response['duration'] = time.perf_counter() - start_time
            response['success'] = response_data['success']
            response['message'] = response_data['message']
        return response
    
    def warm_up_lambda(self, function_name):
        event_data = {'task': 'warmup'}
        return self.invoke_function(function_name, json.dumps(event_data))  # Pass the required payload or input parameters
       