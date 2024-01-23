from typing import Optional, List, Tuple, Callable, Dict
import functools
import json
import os
from pathlib import Path
import boto3
from urllib.parse import urlparse
from data_objects.batch import Batch


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


class Dataset():
    def __init__(self, dataset_id, source_system, data_dir, labelled_samples):
        self.dataset_id = dataset_id
        self.img_extensions = ['.jpg', '.JPG', '.jpeg', '.JPEG', '.png', '.PNG', '.ppm', '.PPM', '.bmp', '.BMP']
        self.source_system = source_system
        self.data_dir = data_dir

        if labelled_samples is not None:
             self.samples = json.loads(labelled_samples)
        else:     
            if source_system =='local':
                self.samples =self._classify_samples_local(data_dir)
            elif source_system == 's3':
                self.samples =self._classify_samples_s3(S3Url(data_dir))

        self.batches: Dict[int, Batch] = {}  # Dictionary to store batch information        
    
    @functools.cached_property
    def _classed_items(self) -> List[Tuple[str, int]]:
        return [
            (blob, class_index)
            for class_index, blob_class in enumerate(self.samples)
            for blob in self.samples[blob_class]
        ]
    
    def get_samples_for_batch(self, batch_sample_indices):
        samples = []
        for i in  batch_sample_indices:
                samples.append(self._classed_items[i])
        return samples
   

    def __len__(self):
        return sum(len(class_items) for class_items in self.samples.values())

    def is_image_file(self, filename:str):
        return any(filename.endswith(extension) for extension in self.img_extensions)
    
    def _classify_samples_local(self, data_dir) -> Dict[str, List[str]]:
        data_dir = str(Path(data_dir))

        img_classes: Dict[str, List[str]] = {}
        index_file = Path(data_dir) / 'index.json'

        if index_file.exists():
            with open(index_file.absolute()) as f:
                img_classes = json.load(f)
        else:
            for dirpath, dirnames, filenames in os.walk(data_dir):
                for filename in filter(self.is_image_file, filenames):
                    
                    img_class = os.path.basename(dirpath.removesuffix('/'))
                    img_path = os.path.join(dirpath, filename)
                    img_classes.setdefault(img_class, []).append(img_path)

            json_object = json.dumps(img_classes, indent=4)
            with open(index_file, "w") as outfile:
                outfile.write(json_object)

        return img_classes
    
    def _classify_samples_s3(self, s3url: S3Url) -> Dict[str, List[str]]:
        s3_client = boto3.client('s3')
        s3_resource = boto3.resource("s3")

        try:
            # Check if 'prefix' folder exists
            response = s3_client.list_objects(Bucket=s3url.bucket, Prefix=s3url.key, Delimiter='/', MaxKeys=1)
            if 'NextMarker' not in response:
                # 'prefix' dir not found. Skipping task
                return None

            # Check if index file in the root of the folder to avoid looping through the entire bucket
            index_object = s3_resource.Object(s3url.bucket, s3url.key + 'index.json')
            try:
                file_content = index_object.get()['Body'].read().decode('utf-8')
                blob_classes = json.loads(file_content)
            except:
                # No index file found, creating it
                blob_classes = self._create_index_file_s3(s3url)

            return blob_classes
        except Exception as e:
                # Handle exceptions, e.g., log them
                print(f"Error in _classify_blobs_s3: {e}")
                return None

    def _create_index_file_s3(self, s3url: S3Url) -> Dict[str, List[str]]:
        import json

        s3_client = boto3.client('s3')
        s3_resource = boto3.resource("s3")

        blob_classes: Dict[str, List[str]] = {}
        paginator = s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=s3url.bucket, Prefix=s3url.key)

        for page in pages:
            for blob in page['Contents']:
                blob_path = blob.get('Key')
                # Check if the object is a folder which we want to ignore
                if blob_path[-1] == "/":
                    continue
                stripped_path = self._remove_prefix(blob_path, s3url.key).lstrip("/")
                # Indicates that it did not match the starting prefix
                if stripped_path == blob_path:
                    continue
                if not self.is_image_file(blob_path):
                    continue
                blob_class = stripped_path.split("/")[0]
                blobs_with_class = blob_classes.get(blob_class, [])
                blobs_with_class.append(blob_path)
                blob_classes[blob_class] = blobs_with_class

        index_object = s3_resource.Object(s3url.bucket, s3url.key + 'index.json')
        index_object.put(Body=(bytes(json.dumps(blob_classes, indent=4).encode('UTF-8'))))

        return blob_classes
