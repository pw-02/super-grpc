import hashlib
import time
import math
from queue import Queue
import threading
import threading
from queue import Empty

class TokenBucket:
    def __init__(self, capacity, refill_rate):
        self.capacity = capacity
        self.tokens = capacity
        self.refill_rate = refill_rate
        self.last_refill_time = time.time()
        self.prefetched_batches = set()  # Keep track of downloaded/prefecthed bacthes
        self.lock = threading.Lock()  # Lock for accessing shared resources
    
    def refill(self, tokens_to_add =1):
        now = time.time()
        delta_time = now - self.last_refill_time
        # tokens_to_add = delta_time * self.refill_rate
        self.tokens = min(self.capacity, self.tokens + tokens_to_add)
        self.last_refill_time = now

    def consume(self, tokens):
        with self.lock:
            if tokens <= self.tokens:
                self.tokens -= tokens
                return True
            else:
                return False

    def batch_prefeteched(self, batch_id):
        with self.lock:
            self.prefetched_batches.add(batch_id)
    
    def batch_accessed(self, batch_id):
        with self.lock:
            if batch_id in self.prefetched_batches:
                self.refill()
                #remove so that a token only gets added to the bucket the first time the batch is accessed
                #without this the lookahead_rate config setting would be inaccurate
                self.prefetched_batches.remove(batch_id)

    def wait_for_tokens(self):
        while not self.consume(1):
            time.sleep(0.1)  # Wait if there are not enough tokens available
            # token_bucket.refill()  # Refill tokens during the wait
    


class CustomQueue(Queue):
    def __iter__(self):
        return iter(self.queue)
    
    def remove_item(self, item, block=True, timeout=None):

        with self.not_empty:
            if not block:
                if not self._qsize():
                    raise Empty
            elif timeout is None:
                while not self._qsize():
                    self.not_empty.wait()
            elif timeout < 0:
                raise ValueError("'timeout' must be a non-negative number")
            else:
                endtime = time() + timeout
                while not self._qsize():
                    remaining = endtime - time()
                    if remaining <= 0.0:
                        raise Empty
                    self.not_empty.wait(remaining)
            if item in self.queue:
                self.queue.remove(item)     
            # item = self._get()
            self.not_full.notify()
            return item


def create_unique_id(int_list):
    # Convert integers to strings and concatenate them
    id_string = ''.join(str(x) for x in int_list)
    
    # Hash the concatenated string to generate a unique ID
    unique_id = hashlib.sha1(id_string.encode()).hexdigest()
    
    return unique_id

def format_timestamp(current_timestamp, use_utc=True):
    if current_timestamp == math.inf:
        return current_timestamp
    if use_utc:
        time_struct = time.gmtime(current_timestamp)
    else:
        time_struct = time.localtime(current_timestamp)

    formatted_current_time = time.strftime("%Y-%m-%d %H:%M:%S", time_struct)
    return formatted_current_time

def remove_trailing_slash(path:str):
    """
    Removes trailing slashes from a directory path.
    """
    path = path.strip()
    if path.endswith('/'):
        return path[:-1]  # Remove the last character (trailing slash)
    if path.endswith('\\'):
        return path[:-1]  # Remove the last character (trailing slash)
    return path