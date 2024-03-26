from utils import create_unique_id
from typing import Dict, List

class Batch:
    def __init__(self, indicies):
        self.indicies: List[int] = indicies
        self.bacth_id:str = create_unique_id(self.indicies)
        self.is_cached:bool = False
        self.next_access_time:float = None
        self.caching_in_progress:bool = False
        self.status:str = False
        self.last_access_time:float = float('inf')

    def set_next_access_time(self, access_time):
        self.next_access_time = access_time
