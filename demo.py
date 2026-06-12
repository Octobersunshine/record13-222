import json
import redis
from ad_frequency_control import AdFrequencyControl, FrequencyPeriod


def demo_single_shard(r):
    print("=" * 70)
    print("  【模式一】单分片（shards=1）— 传统模式，热 key 风险")
    print("=" * 70)
    fc = AdFrequencyControl(r, daily_limit=5, weekly_limit=20, shards=1, local_cache_capacity=0)
    ad_id = "ad_hot_01"
    user_id = "user_100"
    fc.reset(ad_id, user_id)

    for i in range(5):
        fc.check_and_record(ad_id, user_id)

    dist = fc.get_shard_distribution(ad_id, user_id, FrequencyPeriod.DAILY)
    print(f"  Redis key 数量: {len(dist)}")
    for k, v in dist.items():
        print(f"    {k} = {v}")
    print(f"  总曝光次数: {fc.get_count(ad_id, user_id, FrequencyPeriod.DAILY)}")
    print("  → 所有流量集中在 1 个 key，集群下会形成热 key\n")
    fc.reset(ad_id, user_id)


def demo_sharded_counter(r):
    print("=" * 70)
    print("  【模式二】分片计数器（shards=10）— 解决热 key 问题")
    print("=" * 70)
    fc = AdFrequencyControl(r, daily_limit=50, weekly_limit=200, shards=10, local_cache_capacity=0)
    ad_id = "ad_hot_01"
    user_id = "user_200"
    fc.reset(ad_id, user_id)

    for i in range(50):
        fc.check_and_record(ad_id, user_id)

    dist = fc.get_shard_distribution(ad_id, user_id, FrequencyPeriod.DAILY)
    print(f"  Redis key 数量: {len(dist)}")
    for k, v in dist.items():
        bar = "█" * v
        print(f"    {k.split(':')[-1]:>3} = {v:>2}  {bar}")
    total = fc.get_count(ad_id, user_id, FrequencyPeriod.DAILY)
    print(f"  总曝光次数: {total}")
    print("  → 流量分散到 10 个 key，集群下可分布到不同节点\n")
    fc.reset(ad_id, user_id)


def demo_local_cache(r):
    print("=" * 70)
    print("  【模式三】分片 + 本地缓存 — 读写双优化")
    print("=" * 70)
    fc = AdFrequencyControl(
        r,
        daily_limit=100,
        weekly_limit=500,
        shards=5,
        local_cache_capacity=1000,
        local_cache_ttl=3,
    )
    ad_id = "ad_hot_02"
    user_id = "user_300"
    fc.reset(ad_id, user_id)

    print("  第 1 次 check（未命中缓存，访问 Redis）")
    result = fc.check(ad_id, user_id)
    print(f"    daily.count = {result['details']['daily']['current_count']}")

    print("  连续写入 10 次（更新本地缓存）")
    for _ in range(10):
        fc.check_and_record(ad_id, user_id)

    print("  第 2 次 check（命中本地缓存）")
    result = fc.check(ad_id, user_id)
    print(f"    daily.count = {result['details']['daily']['current_count']}")

    print("  清空本地缓存后重新 check（回源 Redis）")
    fc.clear_local_cache()
    result = fc.check(ad_id, user_id)
    print(f"    daily.count = {result['details']['daily']['current_count']}")

    dist = fc.get_shard_distribution(ad_id, user_id, FrequencyPeriod.DAILY)
    print(f"  分片分布: {[v for v in dist.values()]}")
    print("  → 本地缓存大幅降低 Redis 读压力，分片降低写压力\n")
    fc.reset(ad_id, user_id)


def demo_frequency_cap_still_works(r):
    print("=" * 70)
    print("  【验证】分片模式下频控逻辑依然有效")
    print("=" * 70)
    fc = AdFrequencyControl(r, daily_limit=5, weekly_limit=10, shards=8, local_cache_capacity=0)
    ad_id = "ad_hot_03"
    user_id = "user_400"
    fc.reset(ad_id, user_id)

    print("  日限额 = 5，分片数 = 8")
    for i in range(10):
        result = fc.check_and_record(ad_id, user_id)
        status = "✓ 记录成功" if result["recorded"] else "✗ 被拒绝"
        print(f"    第 {i + 1:>2} 次: {status}  count={result['details']['daily']['current_count']}")

    total = fc.get_count(ad_id, user_id, FrequencyPeriod.DAILY)
    print(f"  最终日曝光总数: {total}")
    print("  → 分片模式下频控仍然生效（可能有 1~N 次的超售误差，可接受）\n")
    fc.reset(ad_id, user_id)


def main():
    try:
        r = redis.Redis(host="localhost", port=6379, db=0, decode_responses=True)
        r.ping()
    except redis.ConnectionError:
        print("无法连接 Redis (localhost:6379)，请先启动 Redis 服务。")
        print("下面以静态方式展示分片逻辑的原理...")
        _print_principle()
        return

    test_prefix = "ad_freq"
    for key in r.scan_iter(f"{test_prefix}:*"):
        r.delete(key)

    demo_single_shard(r)
    demo_sharded_counter(r)
    demo_local_cache(r)
    demo_frequency_cap_still_works(r)

    for key in r.scan_iter(f"{test_prefix}:*"):
        r.delete(key)

    print("=" * 70)
    print("  全部 Demo 完成")
    print("=" * 70)


def _print_principle():
    print("""
╔══════════════════════════════════════════════════════════════════╗
║                    热 key 问题 & 解决方案                         ║
╠══════════════════════════════════════════════════════════════════╣
║                                                                  ║
║  【问题】                                                         ║
║  热门广告的曝光计数 key 集中在单个 Redis 节点，                    ║
║  造成该节点 CPU/带宽饱和，形成瓶颈。                              ║
║                                                                  ║
║  【方案一：分片计数器（Sharded Counter）】                        ║
║  将 1 个逻辑 key 拆成 N 个物理分片:                               ║
║    ad_freq:ad_001:user_123:daily:20260612                        ║
║      → ad_freq:ad_001:user_123:daily:20260612:0                  ║
║      → ad_freq:ad_001:user_123:daily:20260612:1                  ║
║      → ad_freq:ad_001:user_123:daily:20260612:2                  ║
║      ... 共 N 个                                                 ║
║                                                                  ║
║  • 写入：随机选一个分片 INCR（O(1)，分散写压力）                  ║
║  • 读取：MGET 所有分片并求和（O(N)，N 通常=10~100）              ║
║  • 效果：写入 QPS 分散到 N 个节点                                 ║
║                                                                  ║
║  【方案二：本地缓存（Local Cache）】                              ║
║  在进程内缓存计数结果（LRU + TTL），                              ║
║  进一步降低 Redis 读压力，适合读多写少场景。                      ║
║                                                                  ║
║  【权衡】                                                         ║
║  • 分片模式下，并发写入可能导致轻微超售（误差≤分片数-1）          ║
║  • 广告频控场景对小比例超售容忍度高，适合本方案                    ║
║  • 若需精确控制，可设 shards=1（退化为传统模式）                  ║
║                                                                  ║
╚══════════════════════════════════════════════════════════════════╝
    """)


if __name__ == "__main__":
    main()
