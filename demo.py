import json
import redis
from ad_frequency_control import AdFrequencyControl, FrequencyPeriod


def _cleanup(r):
    for key in r.scan_iter("ad_freq:*"):
        r.delete(key)
    for key in r.scan_iter("ad_click:*"):
        r.delete(key)


def demo_dynamic_frequency_cap(r):
    print("=" * 70)
    print("  【动态频控】点击过广告的用户阈值降低")
    print("=" * 70)
    fc = AdFrequencyControl(
        r,
        daily_limit=10,
        weekly_limit=30,
        clicked_daily_limit=3,
        clicked_weekly_limit=8,
        shards=1,
        local_cache_capacity=0,
    )
    ad_id = "ad_dyn_01"
    user_id = "user_dyn_100"
    fc.reset(ad_id, user_id)
    fc.reset_click(ad_id, user_id)

    print(f"  日限额: 基础={fc.rules[FrequencyPeriod.DAILY].max_impressions}, "
          f"点击后={fc.rules[FrequencyPeriod.DAILY].clicked_max_impressions}")
    print(f"  周限额: 基础={fc.rules[FrequencyPeriod.WEEKLY].max_impressions}, "
          f"点击后={fc.rules[FrequencyPeriod.WEEKLY].clicked_max_impressions}")
    print()

    print("  —— 阶段 A：用户未点击，正常高阈值曝光 ——")
    for i in range(5):
        r1 = fc.check_and_record(ad_id, user_id)
        limit = r1["details"]["daily"]["limit"]
        count = r1["details"]["daily"]["current_count"]
        tag = " [点击用户阈值]" if r1.get("clicked") else ""
        print(f"    曝光 #{i + 1}: count={count}, limit={limit}{tag}")

    print(f"  用户点击广告前 has_clicked = {fc.has_clicked(ad_id, user_id)}")
    print()

    print("  —— 阶段 B：用户点击广告，记录点击状态 ——")
    was_new = fc.record_click(ad_id, user_id)
    print(f"  record_click 返回 was_new={was_new}")
    print(f"  点击后 has_clicked = {fc.has_clicked(ad_id, user_id)}")
    print()

    print("  —— 阶段 C：点击后阈值立刻降低，继续曝光 ——")
    for i in range(5):
        r1 = fc.check_and_record(ad_id, user_id)
        limit = r1["details"]["daily"]["limit"]
        count = r1["details"]["daily"]["current_count"]
        allowed = r1["allowed"]
        status = "✓ 曝光" if allowed else "✗ 拦截"
        tag = " [点击用户阈值]" if r1.get("clicked") else ""
        print(f"    曝光 #{5 + i + 1}: count={count}, limit={limit}{tag}  → {status}")
        if not allowed:
            break

    final = fc.check(ad_id, user_id)
    print()
    print(f"  最终状态: allowed={final['allowed']}, clicked={final['clicked']}")
    print(f"  daily: count={final['details']['daily']['current_count']} "
          f"/ limit={final['details']['daily']['limit']} "
          f"(base={final['details']['daily']['base_limit']})")
    print(f"  weekly: count={final['details']['weekly']['current_count']} "
          f"/ limit={final['details']['weekly']['limit']} "
          f"(base={final['details']['weekly']['base_limit']})")
    print()
    fc.reset(ad_id, user_id)
    fc.reset_click(ad_id, user_id)


def demo_dynamic_with_sharding(r):
    print("=" * 70)
    print("  【组合】动态频控 + 分片计数器 + 本地缓存")
    print("=" * 70)
    fc = AdFrequencyControl(
        r,
        daily_limit=20,
        weekly_limit=100,
        clicked_daily_limit=5,
        clicked_weekly_limit=20,
        shards=8,
        local_cache_capacity=1000,
        local_cache_ttl=3,
    )
    ad_id = "ad_dyn_02"
    user_id = "user_dyn_200"
    fc.reset(ad_id, user_id)
    fc.reset_click(ad_id, user_id)

    for _ in range(3):
        fc.check_and_record(ad_id, user_id)

    print(f"  点击前: has_clicked={fc.has_clicked(ad_id, user_id)}, "
          f"daily.count={fc.get_count(ad_id, user_id, FrequencyPeriod.DAILY)}")

    fc.record_click(ad_id, user_id)

    for _ in range(5):
        r1 = fc.check_and_record(ad_id, user_id)
        if not r1["allowed"]:
            print(f"  达到点击后阈值 (count={r1['details']['daily']['current_count']}, "
                  f"limit={r1['details']['daily']['limit']}) 停止")
            break

    dist = fc.get_shard_distribution(ad_id, user_id, FrequencyPeriod.DAILY)
    print(f"  分片分布 (共 {len(dist)} 片): {[v for v in dist.values()]}")
    final = fc.check(ad_id, user_id)
    print(f"  最终 daily: count={final['details']['daily']['current_count']} "
          f"/ limit={final['details']['daily']['limit']} "
          f"(base={final['details']['daily']['base_limit']})")
    print()
    fc.reset(ad_id, user_id)
    fc.reset_click(ad_id, user_id)


def demo_reset_click(r):
    print("=" * 70)
    print("  【重置点击】清除点击状态后阈值恢复")
    print("=" * 70)
    fc = AdFrequencyControl(
        r,
        daily_limit=10,
        clicked_daily_limit=2,
        shards=1,
        local_cache_capacity=0,
    )
    ad_id = "ad_dyn_03"
    user_id = "user_dyn_300"
    fc.reset(ad_id, user_id)
    fc.reset_click(ad_id, user_id)

    fc.record_click(ad_id, user_id)
    r1 = fc.check(ad_id, user_id)
    print(f"  点击后: limit={r1['details']['daily']['limit']} (base={r1['details']['daily']['base_limit']})")

    fc.reset_click(ad_id, user_id)
    r2 = fc.check(ad_id, user_id)
    print(f"  重置点击: limit={r2['details']['daily']['limit']} (base={r2['details']['daily']['base_limit']})")
    print()
    fc.reset(ad_id, user_id)


def demo_set_limit_dynamic(r):
    print("=" * 70)
    print("  【动态修改阈值】set_limit 支持点击后阈值")
    print("=" * 70)
    fc = AdFrequencyControl(r, daily_limit=10, clicked_daily_limit=5)
    ad_id = "ad_dyn_04"
    user_id = "user_dyn_400"
    fc.reset(ad_id, user_id)
    fc.reset_click(ad_id, user_id)

    fc.record_click(ad_id, user_id)
    r1 = fc.check(ad_id, user_id)
    print(f"  修改前: limit={r1['details']['daily']['limit']} (base={r1['details']['daily']['base_limit']})")

    fc.set_limit(FrequencyPeriod.DAILY, max_impressions=20, clicked_max_impressions=3)
    r2 = fc.check(ad_id, user_id)
    print(f"  修改后: limit={r2['details']['daily']['limit']} (base={r2['details']['daily']['base_limit']})")
    print()
    fc.reset(ad_id, user_id)
    fc.reset_click(ad_id, user_id)


def demo_batch_check_with_click(r):
    print("=" * 70)
    print("  【批量检查】支持动态频控 (部分广告点击过)")
    print("=" * 70)
    fc = AdFrequencyControl(
        r,
        daily_limit=10,
        weekly_limit=30,
        clicked_daily_limit=2,
        clicked_weekly_limit=6,
        shards=4,
        local_cache_capacity=0,
    )
    ad_ids = ["ad_batch_01", "ad_batch_02", "ad_batch_03"]
    user_id = "user_dyn_500"
    for aid in ad_ids:
        fc.reset(aid, user_id)
        fc.reset_click(aid, user_id)

    for aid in ad_ids:
        for _ in range(3):
            fc.check_and_record(aid, user_id)

    fc.record_click("ad_batch_02", user_id)

    results = fc.batch_check(ad_ids, user_id)
    for aid, res in results.items():
        clicked_str = "点击用户" if res.get("clicked") else "普通用户"
        d = res["details"]["daily"]
        print(f"  {aid}: {clicked_str:6}  count={d['current_count']:>2} "
              f"/ limit={d['limit']:>2} (base={d['base_limit']:>2})  "
              f"allowed={res['allowed']}")

    print()
    for aid in ad_ids:
        fc.reset(aid, user_id)
        fc.reset_click(aid, user_id)


def main():
    try:
        r = redis.Redis(host="localhost", port=6379, db=0, decode_responses=True)
        r.ping()
    except redis.ConnectionError:
        print("无法连接 Redis (localhost:6379)，请先启动 Redis 服务。")
        _print_principle()
        return

    _cleanup(r)

    demo_dynamic_frequency_cap(r)
    demo_dynamic_with_sharding(r)
    demo_reset_click(r)
    demo_set_limit_dynamic(r)
    demo_batch_check_with_click(r)

    _cleanup(r)

    print("=" * 70)
    print("  全部 Demo 完成")
    print("=" * 70)


def _print_principle():
    print("""
╔══════════════════════════════════════════════════════════════════╗
║                     动态频控功能原理                              ║
╠══════════════════════════════════════════════════════════════════╣
║                                                                  ║
║  【目标】点击过广告的用户，频控阈值自动降低，避免广告骚扰。         ║
║                                                                  ║
║  【点击状态存储】                                                 ║
║    key:  ad_click:{ad_id}:{user_id}                              ║
║    TTL:  click_ttl_days (默认 30 天)                             ║
║    语义: 只要 key 存在即表示用户在过去 N 天内点过该广告            ║
║                                                                  ║
║  【阈值规则】                                                     ║
║    FrequencyRule 维护两套阈值:                                    ║
║      • max_impressions            — 未点击用户的基础阈值          ║
║      • clicked_max_impressions    — 点击用户的降级阈值            ║
║    resolve(clicked: bool) → int                                  ║
║                                                                  ║
║  【典型场景】                                                     ║
║    未点击用户: 日/10 次  周/30 次                                 ║
║    点击用户  : 日/3 次   周/8 次   ← 降低 70%                     ║
║                                                                  ║
║  【对外新增 API】                                                 ║
║    record_click(ad_id, user_id)     — 记录点击                   ║
║    has_clicked(ad_id, user_id)      — 查询点击状态               ║
║    reset_click(ad_id, user_id)      — 重置点击状态               ║
║    set_limit(..., clicked_max=...)  — 动态修改点击阈值           ║
║                                                                  ║
║  【返回字段增强】                                                 ║
║    每次 check/check_and_record 返回中新增:                        ║
║      • clicked                    该用户是否点过广告              ║
║      • details.*.limit            实际生效的阈值                  ║
║      • details.*.base_limit       基础阈值 (对比用)               ║
║      • details.*.applied_clicked_threshold  是否使用点击阈值     ║
║                                                                  ║
║  【与分片/本地缓存兼容】                                          ║
║    点击状态与频控计数相互独立，任意组合:                          ║
║    shards=1 + cache=0   → 传统精确频控 + 动态阈值                ║
║    shards=16 + cache=on → 集群大规模场景 + 动态阈值              ║
║                                                                  ║
╚══════════════════════════════════════════════════════════════════╝
    """)


if __name__ == "__main__":
    main()
