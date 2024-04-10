import simpy

class TTLCache:
    def __init__(self, env: simpy.Environment, ttl: int):
        self.env = env
        self.cache = {}
        self.ttl = ttl
        self.total_items = 0
        self.nearing_expiration = set()  # Set to track keys nearing expiration

    def set(self, key):
        expiration_time = self.env.now + self.ttl
        self.cache[key] = expiration_time
        
        # If the key is nearing expiration, remove it from nearing expiration set
        if key in self.nearing_expiration:
            self.nearing_expiration.remove(key)
        
    def get(self, key):
        if key in self.cache and self.cache[key] > self.env.now:
            # Extend TTL if the key is nearing expiration
            if key in self.nearing_expiration:
                self.cache[key] = self.env.now + self.ttl
                self.nearing_expiration.remove(key)  # Remove from nearing expiration set
            return True
        else:
            return False
    
    def update_nearing_expiration(self):
        """
        Method to update nearing_expiration set with keys nearing expiration.
        This method can be called periodically to maintain the set.
        """
        while True:
            yield self.env.timeout(2)  # Call every 5 time units
            now = self.env.now
            nearing_expiration_keys = [key for key, expiration_time in self.cache.items() if expiration_time - now <= self.ttl/2]
            self.nearing_expiration.update(nearing_expiration_keys)


def ml_job(env: simpy.Environment, job_id, cache: TTLCache, access_pattern, training_speed):
    for key in access_pattern:
        yield env.timeout(training_speed)
        if cache.get(key):
            print(f"Job {job_id} hit {key} in cache at time {env.now}s")
        else:
            print(f"Job {job_id} missed {key} in cache at time {env.now}s")

def prefetcher(env: simpy.Environment, cache: TTLCache, access_pattern, lookahead_distance, rate):
    for key in access_pattern[:lookahead_distance]:
        cache.set(key)
        print(f"Prefecther added {key} to cache at time {env.now}s")
    yield env.timeout(0)

    # Maintain look ahead
    for key in access_pattern[lookahead_distance:]:
        yield env.timeout(rate)
        cache.set(key)
        print(f"Prefecther added {key} to cache at time {env.now}s")
        

if __name__ == "__main__":
    env = simpy.Environment()
    cache = TTLCache(env, ttl=4)  # TTL set to 60 seconds
    lookahead = 2
    # Define prefetch pattern for cache users
    prefetch_pattern = [f'key_{i}' for i in range(1, 11)]  # Example prefetch pattern with 500 keys
    
    job_speeds = [1,2]

    # Create processes for adding items to the cache and for cache users
    env.process(prefetcher(env, cache, prefetch_pattern, lookahead, min(job_speeds)))

    for idx, speed in enumerate(job_speeds):
        env.process(ml_job(env, idx+1, cache, prefetch_pattern, speed))
    
    # #Periodically update the nearing_expiration set in the cache
    # env.process(cache.update_nearing_expiration())
    
    # Run the simulation
    env.run()
