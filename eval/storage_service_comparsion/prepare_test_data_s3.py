import boto3
from io import BytesIO
import os

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
    # AWS S3 bucket name
    bucket_name = 'your-bucket-name'

    # Total upload size in GB
    total_upload_size_gb = 5
    # Calculate the total upload size in MB
    total_upload_size_mb = total_upload_size_gb * 1024
    
    # List of file sizes to upload (in MB)
    file_sizes_mb = [0.5, 1, 2, 4, 8, 16, 32]  # Add more sizes as needed
    
    # Calculate the number of uploads for each file size
    num_uploads = {}
    for file_size_mb in file_sizes_mb:
        # Calculate the number of uploads needed to reach the total upload size
        num_uploads[file_size_mb] = int(total_upload_size_mb / file_size_mb)


    # Upload files to S3
    for file_size_mb, uploads in num_uploads.items():
        file_content = create_file_content(file_size_mb)
        local_directory = f'{file_size_mb}_{uploads}'
        os.makedirs(local_directory, exist_ok=True)
        for i in range(uploads):
            # Generate a unique file key for each upload
            # file_key = f'test_folder/test_file_{file_size_mb}mb_{i}.txt'
            file_name = f'test_file_{file_size_mb}mb_{i}.txt'
            # Save the file locally
            save_file_locally(local_directory, file_content, file_name)

            # # Upload the file to S3
            # upload_file_to_s3(bucket_name, file_content, file_key)