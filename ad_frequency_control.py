import random
import threading
import time
from collections import OrderedDict
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Optional, Tuple

import redis


class FrequencyPeriod(Enum):
    DAILY = "daily"
    WEEKLY = "weekly"


class FrequencyRule:
    def __init__(self, period: FrequencyPeriod, max_impressions: int):
        self.period = period
        self.max_impressions = max_impressions


class _LocalLRUCache:
    def __init__(self, capacity: int = 10000, default_ttl: int = 5):
        self.capacity = capacity
        self.default_ttl = default_ttl
        self._cache: "OrderedDict[tuple, Tuple[int, float]]" = OrderedDict()
        self._lock = threading.Lock()

    def get(self, key: tuple) -> Optional[int]:
        with self._lock:
            if key not in self._cache:
                return None
            value, expire_at = self._cache[key]
            if time.time() > expire_at:
                del self._cache[key]
                return None
            self._cache.move_to_end(key)
            return value

    def set(self, key: tuple, value: int, ttl: Optional[int] = None):
        if ttl is None:
            ttl = self.default_ttl
        expire_at = time.time() + ttl
        with self._lock:
            if key in self._cache:
                self._cache.move_to_end(key)
            elif len(self._cache) >= self.capacity:
                self._cache.popitem(last=False)
            self._cache[key] = (value, expire_at)

    def invalidate(self, key: tuple):
        with self._lock:
            self._cache.pop(key, None)

    def increment(self, key: tuple, delta: int = 1, ttl: Optional[int] = None) -> Optional[int]:
        with self._lock:
            if key not in self._cache:
                return None
            value, expire_at = self._cache[key]
            if time.time() > expire_at:
                del self._cache[key]
                return None
            new_value = value + delta
            self._cache[key] = (new_value, expire_at)
            self._cache.move_to_end(key)
            return new_value

    def clear(self):
        with self._lock:
            self._cache.clear()


class AdFrequencyControl:
    KEY_PREFIX = "ad_freq"

    def __init__(
        self,
        redis_client: redis.Redis,
        daily_limit: int = 3,
        weekly_limit: int = 10,
        shards: int = 1,
        local_cache_capacity: int = 10000,
        local_cache_ttl: int = 5,
    ):
        if shards < 1:
            raise ValueError("shards must be >= 1")
        self.redis = redis_client
        self.shards = shards
        self.rules = {
            FrequencyPeriod.DAILY: FrequencyRule(FrequencyPeriod.DAILY, daily_limit),
            FrequencyPeriod.WEEKLY: FrequencyRule(FrequencyPeriod.WEEKLY, weekly_limit),
        }
        self._local_cache = _LocalLRUCache(
            capacity=local_cache_capacity,
            default_ttl=local_cache_ttl,
        ) if local_cache_capacity > 0 else None

    def _build_shard_keys(self, ad_id: str, user_id: str, period: FrequencyPeriod, bucket: str) -> list[str]:
        base = f"{self.KEY_PREFIX}:{ad_id}:{user_id}:{period.value}:{bucket}"
        if self.shards == 1:
            return [base]
        return [f"{base}:{i}" for i in range(self.shards)]

    def _pick_random_shard_key(self, ad_id: str, user_id: str, period: FrequencyPeriod, bucket: str) -> str:
        if self.shards == 1:
            return f"{self.KEY_PREFIX}:{ad_id}:{user_id}:{period.value}:{bucket}"
        shard_idx = random.randint(0, self.shards - 1)
        return f"{self.KEY_PREFIX}:{ad_id}:{user_id}:{period.value}:{bucket}:{shard_idx}"

    def _get_bucket_and_ttl(self, period: FrequencyPeriod) -> tuple[str, int]:
        now = datetime.now(timezone.utc)
        if period == FrequencyPeriod.DAILY:
            bucket = now.strftime("%Y%m%d")
            remaining = datetime(
                now.year, now.month, now.day, tzinfo=timezone.utc
            ) + timedelta(days=1) - now
            ttl = int(remaining.total_seconds()) + 60
            return bucket, ttl
        else:
            iso_year, iso_week, _ = now.isocalendar()
            bucket = f"{iso_year}W{iso_week:02d}"
            start_of_week = datetime.fromisocalendar(iso_year, iso_week, 1, tzinfo=timezone.utc)
            end_of_week = start_of_week + timedelta(weeks=1)
            remaining = end_of_week - now
            ttl = int(remaining.total_seconds()) + 60
            return bucket, ttl

    def _cache_key(self, ad_id: str, user_id: str, period: FrequencyPeriod, bucket: str) -> tuple:
        return (ad_id, user_id, period.value, bucket)

    def _sum_counts_from_redis(self, keys: list[str]) -> int:
        if len(keys) == 1:
            val = self.redis.get(keys[0])
            return int(val or 0)
        values = self.redis.mget(keys)
        return sum(int(v or 0) for v in values)

    def _get_count_with_cache(self, ad_id: str, user_id: str, period: FrequencyPeriod, bucket: str, ttl: int) -> int:
        cache = self._local_cache
        if cache is not None:
            cache_key = self._cache_key(ad_id, user_id, period, bucket)
            cached = cache.get(cache_key)
            if cached is not None:
                return cached

        keys = self._build_shard_keys(ad_id, user_id, period, bucket)
        total = self._sum_counts_from_redis(keys)

        if cache is not None:
            cache.set(cache_key, total, ttl=min(ttl, cache.default_ttl))

        return total

    def check(self, ad_id: str, user_id: str) -> dict:
        result: dict = {"allowed": True, "details": {}}

        for period, rule in self.rules.items():
            bucket, ttl = self._get_bucket_and_ttl(period)
            count = self._get_count_with_cache(ad_id, user_id, period, bucket, ttl)
            allowed = count < rule.max_impressions
            result["details"][period.value] = {
                "current_count": count,
                "limit": rule.max_impressions,
                "allowed": allowed,
                "bucket": bucket,
                "shards": self.shards,
            }
            if not allowed:
                result["allowed"] = False

        return result

    def record(self, ad_id: str, user_id: str) -> dict:
        check_result = self.check(ad_id, user_id)
        if not check_result["allowed"]:
            return {"recorded": False, "reason": "frequency_cap_exceeded", "details": check_result["details"]}

        pipe = self.redis.pipeline(transaction=False)
        for period, rule in self.rules.items():
            bucket, ttl = self._get_bucket_and_ttl(period)
            key = self._pick_random_shard_key(ad_id, user_id, period, bucket)
            pipe.incr(key)
            pipe.expire(key, ttl)
        pipe.execute()

        cache = self._local_cache
        if cache is not None:
            for period, rule in self.rules.items():
                bucket, ttl = self._get_bucket_and_ttl(period)
                cache_key = self._cache_key(ad_id, user_id, period, bucket)
                new_val = cache.increment(cache_key, 1, ttl=min(ttl, cache.default_ttl))
                if new_val is None:
                    cache.invalidate(cache_key)

        updated_details = {}
        for period_value, detail in check_result["details"].items():
            updated_detail = dict(detail)
            updated_detail["current_count"] = detail["current_count"] + 1
            updated_details[period_value] = updated_detail

        return {"recorded": True, "details": updated_details}

    def check_and_record(self, ad_id: str, user_id: str) -> dict:
        if self.shards == 1:
            return self._check_and_record_single(ad_id, user_id)
        return self._check_and_record_sharded(ad_id, user_id)

    def _check_and_record_single(self, ad_id: str, user_id: str) -> dict:
        watch_keys = []
        keys_info: dict = {}
        for period, rule in self.rules.items():
            bucket, ttl = self._get_bucket_and_ttl(period)
            keys = self._build_shard_keys(ad_id, user_id, period, bucket)
            keys_info[period] = {"keys": keys, "bucket": bucket, "ttl": ttl, "limit": rule.max_impressions}
            watch_keys.extend(keys)

        self.redis.watch(*watch_keys)
        try:
            pipe = self.redis.pipeline(transaction=False)
            for period, info in keys_info.items():
                for key in info["keys"]:
                    pipe.get(key)
            raw_values = pipe.execute()

            counts: dict = {}
            idx = 0
            for period, info in keys_info.items():
                total = 0
                for _ in info["keys"]:
                    total += int(raw_values[idx] or 0)
                    idx += 1
                counts[period] = total

            result: dict = {"allowed": True, "recorded": False, "details": {}}
            for period, info in keys_info.items():
                count = counts[period]
                allowed = count < info["limit"]
                result["details"][period.value] = {
                    "current_count": count,
                    "limit": info["limit"],
                    "allowed": allowed,
                    "bucket": info["bucket"],
                    "shards": self.shards,
                }
                if not allowed:
                    result["allowed"] = False

            if not result["allowed"]:
                self.redis.unwatch()
                result["reason"] = "frequency_cap_exceeded"
                return result

            pipe = self.redis.pipeline(transaction=False)
            for period, info in keys_info.items():
                key = self._pick_random_shard_key(ad_id, user_id, period, info["bucket"])
                pipe.incr(key)
                pipe.expire(key, info["ttl"])
            pipe.execute()

            result["recorded"] = True
            for period_value in result["details"]:
                result["details"][period_value]["current_count"] += 1

            cache = self._local_cache
            if cache is not None:
                for period, info in keys_info.items():
                    cache_key = self._cache_key(ad_id, user_id, period, info["bucket"])
                    cache.set(
                        cache_key,
                        result["details"][period.value]["current_count"],
                        ttl=min(info["ttl"], cache.default_ttl),
                    )

            return result

        except redis.WatchError:
            return {"allowed": False, "recorded": False, "reason": "concurrent_conflict"}

    def _check_and_record_sharded(self, ad_id: str, user_id: str) -> dict:
        bucket_info: dict = {}
        for period, rule in self.rules.items():
            bucket, ttl = self._get_bucket_and_ttl(period)
            count = self._get_count_with_cache(ad_id, user_id, period, bucket, ttl)
            bucket_info[period] = {
                "bucket": bucket, "ttl": ttl, "count": count, "limit": rule.max_impressions,
            }
            if count >= rule.max_impressions:
                details = {}
                for p, info in bucket_info.items():
                    details[p.value] = {
                        "current_count": info["count"],
                        "limit": info["limit"],
                        "allowed": info["count"] < info["limit"],
                        "bucket": info["bucket"],
                        "shards": self.shards,
                    }
                return {
                    "allowed": False,
                    "recorded": False,
                    "reason": "frequency_cap_exceeded",
                    "details": details,
                }

        pipe = self.redis.pipeline(transaction=False)
        for period, info in bucket_info.items():
            key = self._pick_random_shard_key(ad_id, user_id, period, info["bucket"])
            pipe.incr(key)
            pipe.expire(key, info["ttl"])
        pipe.execute()

        cache = self._local_cache
        if cache is not None:
            for period, info in bucket_info.items():
                cache_key = self._cache_key(ad_id, user_id, period, info["bucket"])
                new_val = cache.increment(cache_key, 1, ttl=min(info["ttl"], cache.default_ttl))
                if new_val is None:
                    cache.invalidate(cache_key)

        details = {}
        for period, info in bucket_info.items():
            details[period.value] = {
                "current_count": info["count"] + 1,
                "limit": info["limit"],
                "allowed": True,
                "bucket": info["bucket"],
                "shards": self.shards,
            }
        return {"allowed": True, "recorded": True, "details": details}

    def get_count(self, ad_id: str, user_id: str, period: FrequencyPeriod) -> int:
        bucket, ttl = self._get_bucket_and_ttl(period)
        return self._get_count_with_cache(ad_id, user_id, period, bucket, ttl)

    def reset(self, ad_id: str, user_id: str) -> bool:
        pipe = self.redis.pipeline(transaction=False)
        all_keys: list[str] = []
        for period in self.rules:
            bucket, _ = self._get_bucket_and_ttl(period)
            keys = self._build_shard_keys(ad_id, user_id, period, bucket)
            all_keys.extend(keys)
            for key in keys:
                pipe.delete(key)
        results = pipe.execute()

        cache = self._local_cache
        if cache is not None:
            for period in self.rules:
                bucket, _ = self._get_bucket_and_ttl(period)
                cache_key = self._cache_key(ad_id, user_id, period, bucket)
                cache.invalidate(cache_key)

        return any(results)

    def set_limit(self, period: FrequencyPeriod, max_impressions: int):
        self.rules[period] = FrequencyRule(period, max_impressions)

    def clear_local_cache(self):
        if self._local_cache is not None:
            self._local_cache.clear()

    def batch_check(self, ad_ids: list[str], user_id: str) -> dict[str, dict]:
        all_keys: list[str] = []
        meta: list[tuple[str, FrequencyPeriod, int, int]] = []

        for ad_id in ad_ids:
            for period, rule in self.rules.items():
                bucket, ttl = self._get_bucket_and_ttl(period)
                keys = self._build_shard_keys(ad_id, user_id, period, bucket)
                meta.append((ad_id, period, len(keys), rule.max_impressions))
                all_keys.extend(keys)

        values = self.redis.mget(all_keys) if all_keys else []

        results: dict[str, dict] = {ad_id: {"allowed": True, "details": {}} for ad_id in ad_ids}

        idx = 0
        for ad_id, period, num_keys, limit in meta:
            total = 0
            for _ in range(num_keys):
                total += int(values[idx] or 0)
                idx += 1
            bucket, ttl = self._get_bucket_and_ttl(period)
            allowed = total < limit
            results[ad_id]["details"][period.value] = {
                "current_count": total,
                "limit": limit,
                "allowed": allowed,
                "bucket": bucket,
                "shards": self.shards,
            }
            if not allowed:
                results[ad_id]["allowed"] = False

        return results

    def get_shard_distribution(self, ad_id: str, user_id: str, period: FrequencyPeriod) -> dict[str, int]:
        bucket, _ = self._get_bucket_and_ttl(period)
        keys = self._build_shard_keys(ad_id, user_id, period, bucket)
        values = self.redis.mget(keys)
        return {key: int(val or 0) for key, val in zip(keys, values)}
