
import os
from datetime import datetime, timedelta
from contextlib import contextmanager
from loguru import logger
import pickle

class CacheManager:
    def __init__(self, cache_dir: str, cache_duration: timedelta):
        self.cache_dir = cache_dir
        self.cache_duration = cache_duration
        os.makedirs(self.cache_dir, exist_ok=True)
        self.cache_file = os.path.join(self.cache_dir, "cache.pkl")
        self.timestamp_file = os.path.join(self.cache_dir, "timestamp.txt")
        self.result = None

    def get_cache(self):
        # Check if cache file exists and is valid
        if os.path.exists(self.cache_file) and os.path.exists(self.timestamp_file):
            with open(self.timestamp_file, 'r') as f:
                timestamp = datetime.fromisoformat(f.read().strip())
            if datetime.now() - timestamp < self.cache_duration:
                with open(self.cache_file, 'rb') as f:
                    return pickle.load(f)
        return None

    def dump(self):
        if self.result is not None:
            with open(self.cache_file, 'wb') as f:
                pickle.dump(self.result, f)
            with open(self.timestamp_file, 'w') as f:
                f.write(datetime.now().isoformat())

    def set_result(self, data):
        self.result = data

@contextmanager
def cache_data(cache_dir: str, cache_duration: timedelta):
    cache_manager = CacheManager(cache_dir, cache_duration)
    try:
        yield cache_manager
    finally:
        cache_manager.dump()



