import time
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Optional

import redis


class FrequencyPeriod(Enum):
    DAILY = "daily"
    WEEKLY = "weekly"


class FrequencyRule:
    def __init__(self, period: FrequencyPeriod, max_impressions: int):
        self.period = period
        self.max_impressions = max_impressions


class AdFrequencyControl:
    KEY_PREFIX = "ad_freq"

    def __init__(
        self,
        redis_client: redis.Redis,
        daily_limit: int = 3,
        weekly_limit: int = 10,
    ):
        self.redis = redis_client
        self.rules = {
            FrequencyPeriod.DAILY: FrequencyRule(FrequencyPeriod.DAILY, daily_limit),
            FrequencyPeriod.WEEKLY: FrequencyRule(FrequencyPeriod.WEEKLY, weekly_limit),
        }

    def _build_key(self, ad_id: str, user_id: str, period: FrequencyPeriod, bucket: str) -> str:
        return f"{self.KEY_PREFIX}:{ad_id}:{user_id}:{period.value}:{bucket}"

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

    def check(self, ad_id: str, user_id: str) -> dict:
        pipe = self.redis.pipeline(transaction=False)
        keys_info: dict = {}

        for period, rule in self.rules.items():
            bucket, ttl = self._get_bucket_and_ttl(period)
            key = self._build_key(ad_id, user_id, period, bucket)
            keys_info[period] = {"key": key, "bucket": bucket, "ttl": ttl, "limit": rule.max_impressions}
            pipe.get(key)

        counts = pipe.execute()

        result: dict = {"allowed": True, "details": {}}
        for i, (period, info) in enumerate(keys_info.items()):
            count = int(counts[i] or 0)
            allowed = count < info["limit"]
            result["details"][period.value] = {
                "current_count": count,
                "limit": info["limit"],
                "allowed": allowed,
                "bucket": info["bucket"],
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
            key = self._build_key(ad_id, user_id, period, bucket)
            pipe.incr(key)
            pipe.expire(key, ttl)

        pipe.execute()
        return {"recorded": True, "details": check_result["details"]}

    def check_and_record(self, ad_id: str, user_id: str) -> dict:
        pipe = self.redis.pipeline(transaction=True)

        watch_keys = []
        keys_info: dict = {}
        for period, rule in self.rules.items():
            bucket, ttl = self._get_bucket_and_ttl(period)
            key = self._build_key(ad_id, user_id, period, bucket)
            keys_info[period] = {"key": key, "bucket": bucket, "ttl": ttl, "limit": rule.max_impressions}
            watch_keys.append(key)

        self.redis.watch(*watch_keys)
        try:
            pipe = self.redis.pipeline(transaction=False)
            for period, info in keys_info.items():
                pipe.get(info["key"])
            counts = pipe.execute()

            result: dict = {"allowed": True, "recorded": False, "details": {}}
            for i, (period, info) in enumerate(keys_info.items()):
                count = int(counts[i] or 0)
                allowed = count < info["limit"]
                result["details"][period.value] = {
                    "current_count": count,
                    "limit": info["limit"],
                    "allowed": allowed,
                    "bucket": info["bucket"],
                }
                if not allowed:
                    result["allowed"] = False

            if not result["allowed"]:
                self.redis.unwatch()
                result["recorded"] = False
                result["reason"] = "frequency_cap_exceeded"
                return result

            pipe = self.redis.pipeline(transaction=False)
            for period, info in keys_info.items():
                pipe.incr(info["key"])
                pipe.expire(info["key"], info["ttl"])
            pipe.execute()

            result["recorded"] = True
            for period_value in result["details"]:
                result["details"][period_value]["current_count"] += 1
            return result

        except redis.WatchError:
            return {"allowed": False, "recorded": False, "reason": "concurrent_conflict"}

    def get_count(self, ad_id: str, user_id: str, period: FrequencyPeriod) -> int:
        bucket, _ = self._get_bucket_and_ttl(period)
        key = self._build_key(ad_id, user_id, period, bucket)
        val = self.redis.get(key)
        return int(val or 0)

    def reset(self, ad_id: str, user_id: str) -> bool:
        pipe = self.redis.pipeline(transaction=False)
        for period in self.rules:
            bucket, _ = self._get_bucket_and_ttl(period)
            key = self._build_key(ad_id, user_id, period, bucket)
            pipe.delete(key)
        results = pipe.execute()
        return any(results)

    def set_limit(self, period: FrequencyPeriod, max_impressions: int):
        self.rules[period] = FrequencyRule(period, max_impressions)

    def batch_check(self, ad_ids: list[str], user_id: str) -> dict[str, dict]:
        all_keys_info: dict[str, dict] = {}
        pipe = self.redis.pipeline(transaction=False)

        for ad_id in ad_ids:
            all_keys_info[ad_id] = {}
            for period, rule in self.rules.items():
                bucket, ttl = self._get_bucket_and_ttl(period)
                key = self._build_key(ad_id, user_id, period, bucket)
                all_keys_info[ad_id][period] = {
                    "key": key, "bucket": bucket, "ttl": ttl, "limit": rule.max_impressions,
                }
                pipe.get(key)

        counts = pipe.execute()

        idx = 0
        results: dict[str, dict] = {}
        for ad_id in ad_ids:
            results[ad_id] = {"allowed": True, "details": {}}
            for period, info in all_keys_info[ad_id].items():
                count = int(counts[idx] or 0)
                allowed = count < info["limit"]
                results[ad_id]["details"][period.value] = {
                    "current_count": count,
                    "limit": info["limit"],
                    "allowed": allowed,
                    "bucket": info["bucket"],
                }
                if not allowed:
                    results[ad_id]["allowed"] = False
                idx += 1

        return results
