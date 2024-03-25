
import boto3
from urllib.parse import urlparse
import json

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


# test = S3Helper()
# file_path = '/workspaces/super-ml-workloads/language/reports/gpt2-pytorch-classic/version_0/hparams.yaml'
# # Split the path based on the 'reports/' term
# split_path = file_path.split('/reports/', 1)
# # Check if the split was successful
# if len(split_path) > 1:
#     # Use the second part of the split as the trimmed path
#     trimmed_path = split_path[1]
#     print(trimmed_path)
# else:
#     print("Prefix not found in the path.")
# test.upload_to_s3(file_path, 'superreports23',trimmed_path)