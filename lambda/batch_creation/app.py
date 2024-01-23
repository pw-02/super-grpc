import torch
import torchvision
import base64
import boto3
import redis
import zlib
import concurrent.futures
from PIL import Image
from io import BytesIO
import json
import os
# Externalize configuration parameters
#REDIS_HOST = '172.17.0.2'
#REDIS_HOST = 'host.docker.internal' #use this when testing locally on .dev container
#REDIS_PORT = 6379

REDIS_HOST =  "10.0.31.114"
REDIS_PORT = 6378

s3_client = boto3.client('s3')
redis_client = redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT) # Instantiate Redis client

use_local = False

def download_file(bucket_name, file_path):
# Print current working directory
    if use_local:
        os.chdir('/workspaces/super-dl/')
        file_path = os.path.join(os.getcwd(), file_path)
        with open(file_path, 'rb') as file:
            content = file.read()
        return content
    else:
        # Download file into memory
        obj = s3_client.get_object(Bucket=bucket_name, Key=file_path)
        content = obj['Body'].read()
        return content

def process_file(content, transformations):
    image = Image.open(BytesIO(content))
    # Apply transformations if specified
    if transformations is not None:
        processed_tensor = transformations(image)
    else:
        # Convert image to PyTorch tensor without transformations
        processed_tensor =  torchvision.transforms.ToTensor()(image)
    return processed_tensor

def download_and_process_file(bucket_name, sample, transformations):
    sample_path = sample[0]
    sample_label = sample[1]
    content = download_file(bucket_name, sample_path)
    processed_tensor = process_file(content, transformations)
    return processed_tensor, sample_label

def create_torch_batch(bucket_name, batch_metadata, transformations):
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = {executor.submit(download_and_process_file, bucket_name, sample, transformations): sample for sample in batch_metadata}
        sample_list = []
        label_list = []

        for future in concurrent.futures.as_completed(futures):
            file_path = futures[future]
            try:
                processed_tensor, label = future.result()
                sample_list.append(processed_tensor)
                label_list.append(label)

            except Exception as e:
                print(f"Error processing file {file_path}: {e}")
    return torch.stack(sample_list), torch.tensor(label_list)

def deserialize_torchvision_transformation(serialized_transform):
    # Reconstruct the transform object from the serialized representation
    transform_list = []
    for t in serialized_transform:
        transform_class = getattr(torchvision.transforms, t['type'])
        transform_instance = transform_class(**t['args'])
        transform_list.append(transform_instance)
    return torchvision.transforms.Compose(transform_list)


def lambda_handler(event, context):
    try:
        #Extract information from the event
        # body = event['body']
        # event = json.loads(body)

        bucket_name = event['bucket_name']
        batch_metadata = event['batch_metadata']
        batch_id = event['batch_id']
        use_compression = True

        if 'transformations' in event:
            transformations= deserialize_torchvision_transformation(event['transformations']['transform_params'])
        else:
            transformations = None

        #'batch_tensor' contains a batch of PyTorch tensors representing the images
        tensor_batch = create_torch_batch(bucket_name, batch_metadata, transformations)

        # Serialize the PyTorch tensor to binary data, then get the serialized data from the buffer
        buffer = BytesIO()
        torch.save(tensor_batch, buffer)
        serialized_tensor_batch = buffer.getvalue()

        if use_compression:
            serialized_tensor_batch = zlib.compress(serialized_tensor_batch)

        # Encode with base64
        serialized_tensor_batch = base64.b64encode(serialized_tensor_batch).decode('utf-8')

        # cache_batch in redis using batch_id
        redis_client.set(batch_id, serialized_tensor_batch)

        return {
            'statusCode': 200,
            'body': 'Processing completed successfully.'
            #'body': event
            }
    
    except Exception as e:
        return {
            'statusCode': 500,
            'body': f'Error: {str(e)}'
        }