from functools import cached_property
import aws_utils as aws_utils
from typing import List, Tuple, Dict
import functools
from epoch import Epoch
from batch import Batch
from typing import List, Tuple
import numpy as np
from utils import partition_dict


# Define the base class with common properties and methods
class BaseDataset:
    def __init__(self, samples: Dict[str, List[str]], batch_size: int, drop_last: bool):
        self.batch_size = batch_size
        self.drop_last = drop_last
        self.samples = samples
        
        # Calculate the number of batches
        if self.drop_last:
            self.num_batches = len(self) // self.batch_size
        else:
            self.num_batches = (len(self) + self.batch_size - 1) // self.batch_size
    
    @functools.cached_property
    def _classed_items(self) -> List[Tuple[str, int]]:
        return [(blob, class_index)
                for class_index, blob_class in enumerate(self.samples)
                for blob in self.samples[blob_class]]
    
    def __len__(self):
        return sum(len(class_items) for class_items in self.samples.values())
    
    def get_samples(self, indices: List[int]):
        samples = []
        for i in indices:
            samples.append(self._classed_items[i])
        return samples
    

# Dataset class inherits from BaseDataset
class Dataset(BaseDataset):
    def __init__(self, data_dir: str, batch_size: int, drop_last: bool, num_partitions: int = 10, kind = 'vision'):
        # Load samples from data directory
        self.data_dir = data_dir
        if kind == 'vision':
            self.samples = aws_utils.load_paired_s3_object_keys(data_dir, True, True)
        else:
            self.samples = aws_utils.load_paired_s3_object_keys(data_dir, False, False)

        self.bucket_name = aws_utils.S3Url(data_dir).bucket
        
        # Call the base class initializer
        super().__init__(self.samples, batch_size, drop_last)
        
        # Partition the samples and initialize partitions
        partitions = partition_dict(self.samples, num_partitions, batch_size)
        self.partitions: Dict[int, DatasetPartition] = {}
    
        total_len = 0
        
        for idx, data in enumerate(partitions):
            new_partition = DatasetPartition(idx + 1, data, self.batch_size, self.drop_last)
            self.partitions[new_partition.partition_id] = new_partition
            # print(len(new_partition))
            total_len += len(new_partition)
        
        # Ensure total length matches the length of the dataset
        assert total_len == len(self), "Total length does not match the dataset length"

    def summarize(self):
        print("Summary of Dataset:")
        print("Data Directory:", self.data_dir)
        print("Total Files:", len(self))
        print("Total Batches:", self.num_batches)

# DatasetPartition class inherits from BaseDataset
class DatasetPartition(BaseDataset):
    def __init__(self, partition_id: int, samples: Dict[str, List[str]], batch_size: int, drop_last: bool):
        # Call the base class initializer
        super().__init__(samples, batch_size, drop_last)
        
        # Initialize partition-specific properties
        self.partition_id = partition_id
        self.epochs: Dict[int, Epoch] = {}

# The base class `BaseDataset` contains common properties and methods shared by both `Dataset` and `DatasetPartition`.
# Both derived classes call the base class initializer and benefit from the shared logic.