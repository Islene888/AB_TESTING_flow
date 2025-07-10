import time
from Business.events import (
    ARPU,
    ARPPU,
    payment_ratio, LTV, AOV, cancel_sub, payment_rate_all, payment_rate_new, subscribe_new, AOV_new
)

def run_event(event_name, event_func, tag, explanation):
    print(f"\n🚀 开始执行 {event_name} 计算，标签：{tag}")
    print(f"【说明】{explanation}")
    start_time = time.time()
    try:
        event_func(tag)
        print(f"✅ {event_name} 计算完成，耗时：{round(time.time() - start_time, 2)}秒")
    except Exception as e:
        print(f"❌ {event_name} 计算失败，错误信息：{e}")

def main(tag):
    print(f"\n🎬 【主流程启动】标签：{tag}\n")

    events = [
        ("AOV", AOV.main, "7日生命周期价值（LTV）计算，衡量用户在加入后的前7天内所产生的总价值。"),
        ("ARPU", ARPU.main, "每用户平均收入（ARPU）计算，反映每个用户带来的平均收入。"),
        ("ARPPU", ARPPU.main, "每付费用户平均收入（ARPPU）计算，反映每个付费用户产生的收入。"),
        ("cancel_sub", cancel_sub.main, "7日生命周期价值（LTV）计算，衡量用户在加入后的前7天内所产生的总价值。"),
        ("LTV", LTV.main, "7日生命周期价值（LTV）计算，衡量用户在加入后的前7天内所产生的总价值。"),
        ("payment_rate_all", payment_rate_all.main, "7日生命周期价值（LTV）计算，衡量用户在加入后的前7天内所产生的总价值。"),
        ("payment_rate_new", payment_rate_new.main, "7日生命周期价值（LTV）计算，衡量用户在加入后的前7天内所产生的总价值。"),
        ("AOV_new", AOV_new.main, "7日生命周期价值（LTV）计算，衡量用户在加入后的前7天内所产生的总价值。"),
        ("subscribe_new", subscribe_new.main, "7日生命周期价值（LTV）计算，衡量用户在加入后的前7天内所产生的总价值。"),
        ("payment", payment_ratio.main, "支付比例计算完成")
    ]

    for event_name, event_func, explanation in events:
        run_event(event_name, event_func, tag, explanation)

    print("\n🎉 【所有计算处理完毕】")

if __name__ == "__main__":
    tag = "new_ui"
    main(tag)
