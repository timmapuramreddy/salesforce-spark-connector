from threading import Lock
from typing import Dict, Optional
from functools import lru_cache

class FieldCache:
    def __init__(self, cache_size: int = 100):
        self.cache_size = cache_size
        self._object_describe_cache: Dict = {}
        self._field_type_cache: Dict = {}
        self._cache_lock = Lock()

    @lru_cache(maxsize=100)
    def get_object_describe(self, sf, sobject_name: str) -> Dict:
        with self._cache_lock:
            if sobject_name not in self._object_describe_cache:
                describe_result = sf.__getattr__(sobject_name).describe()
                self._object_describe_cache[sobject_name] = describe_result
            return self._object_describe_cache[sobject_name]

    def get_field_type(self, field_key: str) -> Optional[str]:
        with self._cache_lock:
            return self._field_type_cache.get(field_key)

    def set_field_type(self, field_key: str, field_type: str):
        with self._cache_lock:
            if len(self._field_type_cache) >= self.cache_size:
                self._field_type_cache.pop(next(iter(self._field_type_cache)))
            self._field_type_cache[field_key] = field_type

    def clear(self):
        with self._cache_lock:
            self._object_describe_cache.clear()
            self._field_type_cache.clear()
            self.get_object_describe.cache_clear() 