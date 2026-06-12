import json
import redis
from ad_frequency_control import AdFrequencyControl, FrequencyPeriod


def main():
    r = redis.Redis(host="localhost", port=6379, db=0, decode_responses=True)
    r.ping()

    test_prefix = "ad_freq"
    for key in r.scan_iter(f"{test_prefix}:*"):
        r.delete(key)

    fc = AdFrequencyControl(r, daily_limit=3, weekly_limit=7)
    ad_id = "ad_001"
    user_id = "user_123"

    print("=" * 60)
    print("1. 检查初始状态（应允许）")
    print("=" * 60)
    result = fc.check(ad_id, user_id)
    print(json.dumps(result, indent=2, ensure_ascii=False))

    print()
    print("=" * 60)
    print("2. 连续记录曝光 3 次（达到日限额）")
    print("=" * 60)
    for i in range(3):
        result = fc.check_and_record(ad_id, user_id)
        print(f"  第 {i + 1} 次曝光记录: recorded={result['recorded']}")
    print()

    result = fc.check(ad_id, user_id)
    print(f"  当前状态: allowed={result['allowed']}")
    print(f"  日统计: {json.dumps(result['details']['daily'], ensure_ascii=False)}")
    print(f"  周统计: {json.dumps(result['details']['weekly'], ensure_ascii=False)}")

    print()
    print("=" * 60)
    print("3. 尝试第 4 次曝光（应被日限额拒绝）")
    print("=" * 60)
    result = fc.check_and_record(ad_id, user_id)
    print(json.dumps(result, indent=2, ensure_ascii=False))

    print()
    print("=" * 60)
    print("4. 查询当前曝光计数")
    print("=" * 60)
    daily_count = fc.get_count(ad_id, user_id, FrequencyPeriod.DAILY)
    weekly_count = fc.get_count(ad_id, user_id, FrequencyPeriod.WEEKLY)
    print(f"  日曝光次数: {daily_count}, 周曝光次数: {weekly_count}")

    print()
    print("=" * 60)
    print("5. 重置用户曝光记录后再次曝光（应允许）")
    print("=" * 60)
    fc.reset(ad_id, user_id)
    result = fc.check_and_record(ad_id, user_id)
    print(f"  重置后曝光: recorded={result['recorded']}")

    print()
    print("=" * 60)
    print("6. 批量检查多个广告的曝光状态")
    print("=" * 60)
    fc.reset(ad_id, user_id)
    ad_ids = ["ad_001", "ad_002", "ad_003"]
    results = fc.batch_check(ad_ids, user_id)
    for aid, res in results.items():
        print(f"  {aid}: allowed={res['allowed']}")

    print()
    print("=" * 60)
    print("7. 动态修改限额")
    print("=" * 60)
    fc.set_limit(FrequencyPeriod.DAILY, 5)
    result = fc.check(ad_id, user_id)
    print(f"  修改日限额为 5 后: daily limit={result['details']['daily']['limit']}")

    for key in r.scan_iter(f"{test_prefix}:*"):
        r.delete(key)

    print()
    print("Demo 完成")


if __name__ == "__main__":
    main()
