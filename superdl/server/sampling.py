import random
import job

class EndOfEpochException(Exception):
    pass

class RandomSampler:
    def __init__(self, dataset_len: int):
        self.dataset_len = dataset_len
        self.indices = list(range(dataset_len))
        self.current_index = 0

    def __iter__(self):
        random.shuffle(self.indices)  # Shuffle the indices
        return iter(self.indices)

    def __len__(self):
        return self.dataset_len
    
    def __next__(self):
        if self.current_index < self.dataset_len:
            index = self.current_index
            self.current_index += 1
            return index
        else:
            self.current_index = 0  # Reset index for the next epoch
            raise StopIteration


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

class BatchSampler:
    def __init__(self, sampler, batch_size, drop_last=False):
        self.sampler = sampler
        self.batch_size = batch_size
        self.drop_last = drop_last
        self.iterator = iter(self)
        self.current_index = 0
        self.epoch_seed = 1

    def __iter__(self):
        return self

    def __next__(self):
        if self.current_index < len(self):
            batch = []
            for idx in self.sampler:
                batch.append(idx)
                if len(batch) == self.batch_size:
                    self.current_index += 1
                    return job.Batch(batch, self.epoch_seed, self.current_index)
            if len(batch) > 0 and not self.drop_last:
                self.current_index += 1
                return job.Batch(batch, self.epoch_seed, self.current_index)
            raise EndOfEpochException
        else:
            self.current_index = 0  # Reset index for the next epoch
            raise EndOfEpochException

    def __len__(self):
        if self.drop_last:
            return len(self.sampler) // self.batch_size
        else:
            return (len(self.sampler) + self.batch_size - 1) // self.batch_size
    
    def increment_epoch_seed(self):
        self.epoch_seed +=1
        self.sampler.current_index = 0


if __name__ == "__main__":
    batch_size = 64
    batch_sampler_sequential = BatchSampler(SequentialSampler(50000), batch_size)
    Sequential_Sampler = SequentialSampler(500)
    counter = 0
    while True:
        batch = next(batch_sampler_sequential)
        counter += 1
        print(counter,batch.batch_id)

    counter = 1
    for batch in batch_sampler_sequential:
        # print(f'{counter}-{batch.batch_id}')
        print(f'{counter}')

        counter += 1
    print("End of iteration reached")
