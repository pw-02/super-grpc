import random
from job import Batch

class SequentialSampler:
    def __init__(self, dataset_len: int):
        self.dataset_len = dataset_len
        self.indices = list(range(dataset_len))
        self.current_index = 0

    def __iter__(self):
        return self
    
    def __next__(self):
        if self.current_index < self.dataset_len:
            index = self.current_index
            self.current_index += 1
            return index
        else:
            self.current_index = 0  # Reset index for the next epoch
            raise StopIteration

    def __len__(self):
        return self.dataset_len


class RandomSampler:
    def __init__(self, dataset_len: int):
        self.dataset_len = dataset_len
        self.indices = list(range(dataset_len))

    def __iter__(self):
        random.shuffle(self.indices)  # Shuffle the indices
        return iter(self.indices)

    def __len__(self):
        return self.dataset_len


class BatchSampler:
    def __init__(self, sampler, batch_size, drop_last=False):
        self.sampler = sampler
        self.batch_size = batch_size
        self.drop_last = drop_last

    def __iter__(self):
        batch = []
        for idx in self.sampler:
            batch.append(idx)
            if len(batch) == self.batch_size:
                yield Batch(batch)
                batch = []
        if len(batch) > 0 and not self.drop_last:
            yield Batch(batch)

    def __len__(self):
        if self.drop_last:
            return len(self.sampler) // self.batch_size
        else:
            return (len(self.sampler) + self.batch_size - 1) // self.batch_size
   
    