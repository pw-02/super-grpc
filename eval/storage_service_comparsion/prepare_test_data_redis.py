from io import BytesIO
import redis

def create_file_content(file_size_mb):
    # Create a BytesIO object and write some data to it
    return BytesIO(b'0' * int(file_size_mb * 1024 * 1024))  # Convert MB to bytes


if __name__ == "__main__":

    # Total upload size in GB
    total_upload_size_gb = 5
    # Calculate the total upload size in MB
    total_upload_size_mb = total_upload_size_gb * 1024
    
    # List of file sizes to upload (in MB)
    file_sizes_mb = [0.5]  # Add more sizes as needed
    
    # Calculate the number of uploads for each file size
    num_uploads = {}
    for file_size_mb in file_sizes_mb:
        # Calculate the number of uploads needed to reach the total upload size
        num_uploads[file_size_mb] = int(total_upload_size_mb / file_size_mb)
    
    # Redis connection
    redis_host = 't1.rdior4.ng.0001.usw2.cache.amazonaws.com'
    redis_port = 6379
    redis_db = 0
    redis_client = redis.StrictRedis(host=redis_host, port=redis_port, db=redis_db)

    # Upload files to S3
    for file_size_mb, uploads in num_uploads.items():
        file_content = create_file_content(file_size_mb)
       
        for i in range(uploads):
            # Generate a unique file key for each upload
            file_key = f'test_file_{file_size_mb}mb_{i}.txt'

            # Save the file content in Redis
            #redis_key = f'file_content:{file_key}'
            redis_client.set(file_key, file_content.getvalue())
            print(file_key)