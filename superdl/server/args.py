from dataclasses import dataclass
from typing import Optional
from pathlib import Path

@dataclass
class SUPERArgs:
    s3_data_dir: str = 's3://sdl-cifar10/train/'
    batch_creation_lambda: str = None 
    batch_size: int = 100 
    drop_last:bool = False
    simulate_mode:bool = False
    keep_alive_ping_iterval:int = 900
    max_lookahead_batches:int = 5
    max_prefetch_workers:int = 2
    cache_address:str = None 