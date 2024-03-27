import time
from logger_config import logger
from queue import Queue
import threading

class TokenBucket:
    def __init__(self, capacity, refill_rate):
        self.capacity = capacity
        self.tokens = capacity
        self.refill_rate = refill_rate
        self.last_refill_time = time.time()
    
    def refill(self):
        now = time.time()
        delta_time = now - self.last_refill_time
        tokens_to_add = delta_time * self.refill_rate
        self.tokens = min(self.capacity, self.tokens + tokens_to_add)
        self.last_refill_time = now

    def consume(self, tokens):
        if tokens <= self.tokens:
            self.tokens -= tokens
            return True
        else:
            return False
    
def downloader(token_bucket:TokenBucket, file_queue:Queue):
    while True:
        token_bucket.refill()  # Refill the token bucket
        file_path = file_queue.get()  # Get the file path from the queue
        if token_bucket.consume(1):  # Check if enough tokens are available
            # Download the file from S3
            download_file_from_s3(file_path)
            print(f"File downloaded: {file_path}")
        else:
            # Wait if there are not enough tokens available
            time.sleep(0.1)

def download_file_from_s3(file_path):
    # Initialize Boto3 S3 client
    time.sleep(0.2)
    return

def monitor_ml_job_speed():
    # Placeholder function to monitor the processing speed of the ML job
    # You should implement this function according to your specific ML job setup
    # This function may periodically update the refill rate of the token bucket
    return 10  # Placeholder value for the processing speed

# Example usage
if __name__ == "__main__":
    initial_refill_rate = 10  # Initial refill rate
    token_bucket = TokenBucket(capacity=100, initial_refill_rate=initial_refill_rate)  # Initial refill rate

    # Initialize file queue with file paths to be downloaded
    file_queue = Queue()
    for i in range(20):
        file_queue.put(f"file_{i}.txt")
    
    # Start downloader thread
    downloader_thread = threading.Thread(target=downloader, args=(token_bucket, file_queue))
    downloader_thread.start()
    
    while True:
        # Monitor the processing speed of the ML job
        processing_speed = monitor_ml_job_speed()
        
        # Adjust the refill rate based on the processing speed
        token_bucket.refill_rate = processing_speed * 1.1  # Set refill rate slightly higher than processing speed
        
        time.sleep(1)  # Adjust the refill rate periodically