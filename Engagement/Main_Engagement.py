import time
from Engagement.Events import (
    Continue,
    Follow,
    New_Conversation,
    Message,
    Regen,
    Conversation_reset, edit
)


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
        ("Conversation", New_Conversation.main),
        ("Message", Message.main),
        ("Regen", Regen.main),
        ("edit", edit.main),
        ("ConversationEnded", Conversation_reset.main),
        ("Continue", Continue.main),
        ("Follow", Follow.main)
    ]

    for event_name, event_func in events:
        run_event(event_name, event_func, tag)

    print("\n🎉 【所有事件处理完毕】")


if __name__ == "__main__":
    tag = "chat_0519"  # 未来可以从外部传入或读取配置
    main(tag)
