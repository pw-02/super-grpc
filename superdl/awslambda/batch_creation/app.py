import base64
import json
import zlib
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor, TimeoutError
from io import BytesIO
import boto3
import redis
import torch
import torchvision
from PIL import Image
from queue import Queue, Empty
# Externalize configuration parameters
#REDIS_HOST = '172.17.0.2'
#REDIS_HOST = 'host.docker.internal' #use this when testing locally on .dev container
#REDIS_PORT = 6379

# Configuration parameters
# REDIS_HOST = "localhost"
# REDIS_PORT = 6379
s3_client = None
redis_client = None


def dict_to_torchvision_transform(transform_dict):
    """
    Converts a dictionary of transformations to a PyTorch transform object.
    """
    transform_list = []
    for transform_name, params in transform_dict.items():
        if transform_name == 'Resize':
            transform_list.append(torchvision.transforms.Resize(params))
        elif transform_name == 'Normalize':
            transform_list.append(torchvision.transforms.Normalize(mean=params['mean'], std=params['std']))
        elif params is None:
            transform_list.append(getattr(torchvision.transforms, transform_name)())
        else:
            raise ValueError(f"Unsupported transform: {transform_name}")

    return torchvision.transforms.Compose(transform_list)

def is_image_file(path: str):
    return any(path.endswith(extension) for extension in ['.jpg', '.JPG', '.jpeg', '.JPEG', '.png', '.PNG', '.ppm', '.PPM', '.bmp', '.BMP'])
    
def get_data_sample(bucket_name, data_sample,transformations):


    sample_path, sample_label = data_sample
    obj = s3_client.get_object(Bucket=bucket_name, Key=sample_path)

    if is_image_file(sample_path):
        content = obj['Body'].read()
        content = Image.open(BytesIO(content))
        if content.mode == "L":
            content = content.convert("RGB") 
    else:
        content = obj['Body'].read().decode('utf-8')

    if transformations:
        return transformations(content), sample_label
    else:
        return torchvision.transforms.ToTensor()(content), sample_label

def create_minibatch(bucket_name, samples, transformations):
    sample_data, sample_labels = [], []
    with ThreadPoolExecutor() as executor:
        futures = {executor.submit(get_data_sample, bucket_name, sample, transformations): sample for sample in samples}
        for future in concurrent.futures.as_completed(futures):
            file_path = futures[future]
            try:
                processed_tensor, label = future.result()
                sample_data.append(processed_tensor)
                sample_labels.append(label)
            except Exception as e:
                print(f"Error processing file {file_path}: {e}")

    minibatch = torch.stack(sample_data), torch.tensor(sample_labels)
    
    #Serialize the PyTorch tensor
    buffer = BytesIO()
    torch.save(minibatch, buffer)
    # serialized_mini_batch = buffer.getvalue()
    minibatch = zlib.compress(buffer.getvalue()) #use_compression:

    # Encode the serialized tensor with base64
    minibatch = base64.b64encode(minibatch).decode('utf-8')

    return minibatch

# Define a timeout handler function
def timeout_handler(signum, frame):
    raise TimeoutError("Redis set operation timed out")

def lambda_handler(event, context):
    """
    AWS Lambda handler function that processes a batch of images from an S3 bucket and caches the results in Redis.
    """
    try:
        task = event['task']
        if task == 'warmup':
            return {'success': True, 'message': 'function warmed'}
         
        bucket_name = event['bucket_name']
        batch_samples = event['batch_samples']
        batch_id = event['batch_id'] 
        cache_address = event['cache_address']
        # transformations = event['transformations']
        transformations =  None
        cache_host, cache_port = cache_address.split(":")

        global s3_client, redis_client

        if s3_client is None:
            s3_client = boto3.client('s3')

        if redis_client is None:
            redis_client = redis.StrictRedis(host=cache_host, port=cache_port) # Instantiate Redis client
           
        if task == 'vision':
             #deserailize transfor,ations
            if transformations:
                transformations = dict_to_torchvision_transform(json.loads(transformations))
            torch_minibatch = create_minibatch(bucket_name, batch_samples, transformations)

        # elif task == 'language':
        #     transformations = event.get('transformations')
        #     if transformations:
        #         transformations = dict_to_torchvision_transform(json.loads(transformations))
        #     torch_minibatch = create_minibatch(bucket_name, batch_samples, transformations)

        # Cache minibatch in Redis using batch_id as the key
        redis_client.set(batch_id, torch_minibatch)
        return {
            'success': True,
            'batch_id': batch_id,
            'is_cached': True,
            'message': f"Successfully cached minibatch '{batch_id}'"
            }
    except Exception as e:
       return {
            'success': False,
            'batch_id': batch_id,
            'is_cached': False,
            'message': f"{str(e)}"
        }
