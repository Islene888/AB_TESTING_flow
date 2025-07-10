import time
from chat_click_show.active import  start_chat_rate_2, Time_spent,  \
     click_rate_1, chat_round_3, avg_bot_click_4, first_chat_bot_5
from chat_click_show.explore import show_click_rate_1_3, explore_start_chat_rate_2, Chat_round_4
from growthbook_fetcher.growthbook_data_ETL import fetch_and_save_experiment_data

fetch_and_save_experiment_data()

def run_event(event_name, event_func, tag):
    print(f"\n🚀 开始执行 {event_name} 事件，标签：{tag}")
    start_time = time.time()
    try:
        event_func(tag)
        print(f"✅ {event_name} 事件执行完成，耗时：{round(time.time() - start_time, 2)}秒")
    except Exception as e:
        print(f"❌ {event_name} 事件执行失败，错误信息：{e}")


def main(tag):
    print(f"\n🎬 【主流程启动】标签：{tag}\n")

    events = [
        ("click_rate_1", click_rate_1.main),
        # ("start_chat_rate_2", start_chat_rate_2.main),
        ("chat_round_3", chat_round_3.main),
        ("avg_bot_click_4", avg_bot_click_4.main),
        ("first_chat_bot_5", first_chat_bot_5.main),
        ("Time_spent", Time_spent.main),

        ("show_click_rate_1_3", show_click_rate_1_3.main),
        ("explore_start_chat_rate_2", explore_start_chat_rate_2.main),
        ("Chat_depth_4", Chat_round_4.main)
    ]

    for event_name, event_func in events:
        run_event(event_name, event_func, tag)

    print("\n🎉 【所有事件处理完毕】")


if __name__ == "__main__":

    tag = "pool_rank_exp"  # 未来可以从外部传入或读取配置
    fetch_and_save_experiment_data()
    main(tag)
