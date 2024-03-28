from functools import cached_property
import superdl.awsutils as awsutils
from typing import List, Tuple, Dict
import functools

class Dataset():
    def __init__(self, data_dir:str):
        # self.batch_size:int = batch_size
        # self.drop_last:bool = drop_last
        self.data_dir = data_dir
        self.samples: Dict[str, List[str]] = awsutils.load_paired_s3_object_keys(data_dir, True, True)
        self.bucket_name = awsutils.S3Url(data_dir).bucket
        
    
    @functools.cached_property
    def _classed_items(self) -> List[Tuple[str, int]]:
        return [(blob, class_index)
                for class_index, blob_class in enumerate(self.samples)
                for blob in self.samples[blob_class]
                ]

    def __len__(self):
        return sum(len(class_items) for class_items in self.samples.values())
    
    def get_samples(self, indices: List[int]):
        samples = []
        for i in indices:
            samples.append(self._classed_items[i])
        return samples
    
    def summarize(self):
        print("Summary of Dataset:")
        print("Data Directory:", self.data_dir)
        print("Total Files:", len(self))
