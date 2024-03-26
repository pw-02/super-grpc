import hashlib
import time
import math
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