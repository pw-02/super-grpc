import boto3
from io import BytesIO
import os
import redis

def save_file_locally(directory, file_content, file_name):
    file_path = os.path.join(directory, file_name)
    with open(file_path, 'wb') as f:
        f.write(file_content.getvalue())


def upload_file_to_s3(bucket_name, file_content, file_key):
    s3 = boto3.client('s3')
    try:
        response = s3.put_object(Bucket=bucket_name, Key=file_key, Body=file_content)
        print(f"File uploaded successfully to S3 with ETag: {response['ETag']}")
    except Exception as e:
        print(f"Error uploading file to S3: {str(e)}")


def create_file_content(file_size_mb):
    # Create a BytesIO object and write some data to it
    return BytesIO(b'0' * int(file_size_mb * 1024 * 1024))  # Convert MB to bytes

if __name__ == "__main__":
    import json
    # Configuration
    upload_to_s3 = False
    upload_to_redis = True

    # Amazon S3 Bucket
    s3_bucket_name = "storage.services.bench.data"
    if upload_to_redis:
        # Redis connection
        redis_host = '34.221.25.251'
        redis_port = '6378'
        redis_client = redis.StrictRedis(host=redis_host, port=redis_port)
    
    # Total upload size
    total_upload_size_gb = 200  # Total upload size in GB
    total_upload_size_mb = total_upload_size_gb * 1024  # Convert GB to MBls
    
    # List of file sizes to upload (in MB)
    # [1,4,8,16,32,64]
    file_sizes_mb = [512]  # Add more sizes as needed
    
    # Calculate the number of uploads needed to reach the total upload size
    num_uploads = {}
    for file_size_mb in file_sizes_mb:
        num_uploads[file_size_mb] = int(total_upload_size_mb / file_size_mb)

    for file_size_mb, uploads in num_uploads.items():
        file_content = create_file_content(file_size_mb)
        # local_directory = f'{file_size_mb}_{uploads}'
        # os.makedirs(local_directory, exist_ok=True)
        file_keys = []
        for i in range(uploads):
            if upload_to_s3:
                file_key = f'{file_size_mb}_{uploads}/test_file_{file_size_mb}mb_{i}.txt'
                #upload_file_to_s3(s3_bucket_name, file_content, file_key)
            if upload_to_redis:
                file_key = f'test_file_{file_size_mb}mb_{i}.txt'
                file_keys.append(file_key)
                redis_client.set(file_key, file_content.getvalue())
                print(file_key)
        # Write the JSON string to a file
        with open(f'{file_size_mb}mb_file_keys.json', 'w') as json_file:
            json_file.write(json.dumps(file_keys))