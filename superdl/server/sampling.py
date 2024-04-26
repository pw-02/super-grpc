import random
from batch import Batch

class EndOfEpochException(Exception):
    pass

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
        self.shuffle_indices()

    def shuffle_indices(self):
        random.shuffle(self.indices)
        self.current_index = 0

    def __iter__(self):
        self.shuffle_indices()  # Shuffle indices at the start of each epoch
        return self
    
    def __next__(self):
        if self.current_index < self.dataset_len:
            index = self.indices[self.current_index]
            self.current_index += 1
            return index
        else:
            raise StopIteration

    def __len__(self):
        return self.dataset_len


class BatchSampler:
    def __init__(self, size, batch_size, seed, shuffle=True, drop_last=False):
        if shuffle:
            self.sampler = RandomSampler(size)
        else:
            self.sampler = SequentialSampler(size)
        
        self.batch_size = batch_size
        self.drop_last = drop_last
        self.seed = seed
        self.current_index = 0

    def __iter__(self):
        return self

    def __next__(self):
        if self.current_index >= len(self):
            raise EndOfEpochException

        batch = []
        while len(batch) < self.batch_size:
            try:
                batch.append(next(self.sampler))
            except StopIteration:
                break
        
        if len(batch) == 0:
            raise EndOfEpochException

        self.current_index += 1
        return Batch(batch, self.seed, self.current_index)

    def __len__(self):
        total_batches = len(self.sampler) // self.batch_size
        if not self.drop_last and len(self.sampler) % self.batch_size != 0:
            total_batches += 1
        return total_batches

    def increment_epoch_seed(self):
        self.seed += 1


if __name__ == "__main__":
    batch_size = 128
    size = 50000
    initial_seed = 42
    num_epochs = 10  # Specify the number of epochs you want to run

    for epoch in range(num_epochs):
        print(f"Starting epoch {epoch + 1}")

        # Initialize a BatchSampler with a random sampler for each epoch
        batch_sampler_random = BatchSampler(size, batch_size, initial_seed + epoch, shuffle=True, drop_last=False)
        try:
            # Iterate through each batch in the current epoch
            for counter, batch in enumerate(batch_sampler_random, start=1):
                print(f'Epoch {epoch + 1}, Batch {counter} - Batch ID: {batch.batch_id}, Batch Size {len(batch.indicies)}')
                # Process the batch here (e.g., training, validation, etc.)
                # Example: print(batch.data) if you want to see batch data
        except EndOfEpochException:
            print(f"End of epoch {epoch + 1} reached\n")

        # Increment epoch seed if necessary
        # (You could also choose to use the same seed every epoch or modify the seed as needed)

    print("All epochs completed.")
