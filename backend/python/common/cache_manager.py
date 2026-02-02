"""
Redis cache manager for large data payloads (1+ MB).
Features mandatory compression and optimized TTL for memory efficiency.
"""

import json
import gzip
import logging
from typing import Any, Optional
import redis


logger = logging.getLogger(__name__)


# TTL optimization profiles (in seconds)
TTL_PROFILES = {
    'api_response': 600,        # 10 minutes for API responses
    'user_session': 3600,       # 1 hour for user sessions
    'static_data': 86400,       # 24 hours for static data
    'database_query': 900,      # 15 minutes for database queries
    'realtime_data': 60,        # 1 minute for real-time data
    'large_payload': 300,       # 5 minutes for large payloads (1+ MB) - shorter to prevent memory issues
}


class CacheManager:
    """
    Redis cache manager optimized for large data payloads (1+ MB).

    Features:
    - Mandatory gzip compression for all data (1+ MB payloads)
    - TTL optimization by data type
    - Shorter TTL for large payloads to prevent Redis memory issues
    - Namespace support for key organization
    - Memory-efficient serialization
    """

    def __init__(
        self,
        redis_client: redis.Redis,
        default_ttl: int = 3600,
        namespace: str = "datalayer",
        compression_threshold: int = 1024,
        always_compress: bool = False  # Set to True for 1+ MB payloads
    ):
        """
        Initialize cache manager.

        Args:
            redis_client: Redis client instance
            default_ttl: Default TTL in seconds (default: 3600 = 1 hour)
            namespace: Namespace for cache keys (default: "datalayer")
            compression_threshold: Compress data above this size in bytes (default: 1KB)
            always_compress: Always compress regardless of size (recommended for 1+ MB payloads)
        """
        self.redis = redis_client
        self.default_ttl = default_ttl
        self.namespace = namespace
        self.compression_threshold = compression_threshold
        self.always_compress = always_compress

        logger.info(
            f"Cache manager initialized: namespace={namespace}, "
            f"default_ttl={default_ttl}s, always_compress={always_compress}"
        )

    def _generate_key(self, key: str, namespace: Optional[str] = None) -> str:
        """
        Generate namespaced cache key.

        Args:
            key: Original key
            namespace: Optional namespace override

        Returns:
            str: Namespaced key
        """
        ns = namespace or self.namespace
        return f"{ns}:{key}"

    def _serialize_value(self, value: Any) -> bytes:
        """
        Serialize and optionally compress value.

        Args:
            value: Value to serialize

        Returns:
            bytes: Serialized (and possibly compressed) value
        """
        try:
            # Serialize to JSON
            json_data = json.dumps(value, default=str)
            data_bytes = json_data.encode('utf-8')

            # Determine if compression should be applied
            should_compress = (
                self.always_compress or
                len(data_bytes) > self.compression_threshold
            )

            if should_compress:
                compressed = gzip.compress(data_bytes)
                # Only use compression if it actually reduces size
                if len(compressed) < len(data_bytes):
                    logger.debug(
                        f"Compressed {len(data_bytes)} -> {len(compressed)} bytes "
                        f"({len(compressed)/len(data_bytes)*100:.1f}%)"
                    )
                    return b'GZIP:' + compressed

            return data_bytes

        except Exception as e:
            logger.error(f"Serialization error: {e}")
            raise

    def _deserialize_value(self, data: bytes) -> Any:
        """
        Deserialize and decompress value.

        Args:
            data: Serialized data

        Returns:
            Any: Deserialized value
        """
        try:
            # Check for compression marker
            if data.startswith(b'GZIP:'):
                data = gzip.decompress(data[5:])

            json_str = data.decode('utf-8')
            return json.loads(json_str)

        except Exception as e:
            logger.error(f"Deserialization error: {e}")
            raise

    def _get_ttl_for_type(self, cache_type: str) -> int:
        """
        Get optimized TTL based on cache type.

        Args:
            cache_type: Type of cache (api_response, large_payload, etc.)

        Returns:
            int: TTL in seconds
        """
        return TTL_PROFILES.get(cache_type, self.default_ttl)

    def is_available(self) -> bool:
        """
        Check if Redis is available.

        Returns:
            bool: True if Redis is responsive
        """
        if not self.redis:
            return False

        try:
            self.redis.ping()
            return True
        except Exception as e:
            logger.warning(f"Redis not available: {e}")
            return False

    def get(self, key: str, namespace: Optional[str] = None) -> Optional[Any]:
        """
        Get value from cache.

        Args:
            key: Cache key
            namespace: Optional namespace override

        Returns:
            Optional[Any]: Cached value or None if not found
        """
        if not self.is_available():
            return None

        cache_key = self._generate_key(key, namespace)

        try:
            data = self.redis.get(cache_key)

            if data is None:
                logger.debug(f"Cache miss: {cache_key}")
                return None

            value = self._deserialize_value(data)
            logger.debug(f"Cache hit: {cache_key}")
            return value

        except Exception as e:
            logger.error(f"Cache get error for key {cache_key}: {e}")
            return None

    def set(
        self,
        key: str,
        value: Any,
        ttl: Optional[int] = None,
        cache_type: str = 'api_response',
        namespace: Optional[str] = None
    ) -> bool:
        """
        Set value in cache with TTL optimization.

        Args:
            key: Cache key
            value: Value to cache
            ttl: Optional TTL in seconds (uses cache_type TTL if not provided)
            cache_type: Type of cache for TTL optimization (default: 'api_response')
            namespace: Optional namespace override

        Returns:
            bool: True if successful
        """
        if not self.is_available():
            return False

        cache_key = self._generate_key(key, namespace)

        try:
            # Get optimized TTL
            if ttl is None:
                ttl = self._get_ttl_for_type(cache_type)

            # Serialize and compress
            data = self._serialize_value(value)

            # Set in Redis
            self.redis.setex(cache_key, ttl, data)

            logger.debug(
                f"Cache set: {cache_key} (size: {len(data)} bytes, ttl: {ttl}s, type: {cache_type})"
            )
            return True

        except Exception as e:
            logger.error(f"Cache set error for key {cache_key}: {e}")
            return False

    def delete(self, key: str, namespace: Optional[str] = None) -> bool:
        """
        Delete key from cache.

        Args:
            key: Cache key
            namespace: Optional namespace override

        Returns:
            bool: True if key was deleted
        """
        if not self.is_available():
            return False

        cache_key = self._generate_key(key, namespace)

        try:
            result = self.redis.delete(cache_key)
            deleted = result > 0

            if deleted:
                logger.debug(f"Cache delete: {cache_key}")

            return deleted

        except Exception as e:
            logger.error(f"Cache delete error for key {cache_key}: {e}")
            return False

    def invalidate_pattern(self, pattern: str, namespace: Optional[str] = None) -> int:
        """
        Invalidate all keys matching pattern.

        Args:
            pattern: Key pattern (e.g., "user:*")
            namespace: Optional namespace override

        Returns:
            int: Number of keys deleted
        """
        if not self.is_available():
            return 0

        ns = namespace or self.namespace
        full_pattern = f"{ns}:{pattern}"

        try:
            keys = self.redis.keys(full_pattern)
            if keys:
                deleted = self.redis.delete(*keys)
                logger.info(f"Invalidated {deleted} keys matching pattern: {full_pattern}")
                return deleted
            return 0

        except Exception as e:
            logger.error(f"Pattern invalidation error for {full_pattern}: {e}")
            return 0
