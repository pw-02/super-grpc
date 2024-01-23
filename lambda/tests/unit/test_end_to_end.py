from super_grpc_service.aws_lambda.batch_creation import app
import redis
import torch
import base64
from io import BytesIO
import zlib
import requests
import json

def event(batch_id = 1,base_path=('train/Airplane/attack_aircraft_s_001210.png',0),batch_size=1, bucket_name='sdl-cifar10'):
    return {
        'bucket_name': bucket_name,
        'batch_id': batch_id,
        'batch_metadata': [base_path] * batch_size,
        # 'transformations': {
        #     'transform_params': [
        #        {'type': 'Resize', 'args': {'size': (224, 224)}},
        #         {'type': 'ToTensor', 'args': {}},
        #         # Add more transformations as needed
        #     ]
        # }
    }


def test_create_batch_local_api():
    
    redis_client = redis.StrictRedis(host='localhost', port=6379) # Instantiate Redis client
    redis_client.flushdb()
    batchid = 1

    # Replace with the URL of your locally running SAM application
    sam_local_url = 'http://localhost:3000'  # Adjust the port as needed
    sam_function_path = '/create_batch'  # Replace with the actual path

    base_path = '/workspaces/super-dl/datasets/vision/cifar-10/train/Airplane/twinjet_s_001513.png',0
    batch_id = 1
    num_samples =  1
    payload_data = event(batch_id, base_path, num_samples)
    #payload_data = json.dumps(payload_data)

    try:
        # Make an HTTP POST request to invoke your SAM function
        response = requests.post(f"{sam_local_url}{sam_function_path}", json=payload_data)

        # Check the response status code
        if response.status_code == 200:
            # Parse and print the response JSON
            print(response)
        else:
            print(f"Error invoking SAM function. Status code: {response.status_code}")

    except Exception as e:
        print(f"Error invoking SAM function: {e}")
    
    batch_samples, batch_labels  = decode_cached_bacth(redis_client.get(batchid))
    pass
   

def decode_cached_bacth(serialized_tensor_batch):
    decoded_data = base64.b64decode(serialized_tensor_batch) # Decode base64
    try:
        decoded_data = zlib.decompress(decoded_data)
    except:
        pass
      
    buffer = BytesIO(decoded_data)
    batch_samples, batch_labels = torch.load(buffer)
    return  batch_samples, batch_labels 


def test_end_to_end_inc_cache():
    redis_client = redis.StrictRedis(host='localhost', port=6379) # Instantiate Redis client
    base_path = 'datasets/vision/cifar-10/train/Airplane/aeroplane_s_000004.png',0
    batch_id = 1
    num_samples =  1
    new_event = event(batch_id, base_path, num_samples)
    response = app.lambda_handler(new_event, None)
    serialized_tensor_batch = redis_client.get(batch_id)
    decoded_data = base64.b64decode(serialized_tensor_batch) # Decode base64
    try:
        decoded_data = zlib.decompress(decoded_data)
    except:
        pass
      
    buffer = BytesIO(decoded_data)
    batch_samples, batch_labels = torch.load(buffer)
    pass




if __name__ == '__main__':
 
 new_event = event()


 #test_end_to_end_inc_cache()
 test_create_batch_local_api()