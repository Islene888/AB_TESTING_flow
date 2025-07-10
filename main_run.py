import warnings
from symbol import subscript

from  urllib3.exceptions import NotOpenSSLWarning

from Business import Main_business
from Engagement import Main_Engagement
from Recharge import recharge, recharge_summury
from Retention import Main_Retention
from Subscribe import subscribe, sub, first_new_sub
from chat_click_show import Main_Chat_click_show
from growthbook_fetcher.growthbook_data_ETL import fetch_and_save_experiment_data

warnings.filterwarnings("ignore", category=NotOpenSSLWarning)
import warnings
from sqlalchemy.exc import SAWarning
warnings.filterwarnings("ignore", category=SAWarning)

# 获取并保存 GzrowthBook 实验数据
fetch_and_save_experiment_data()

# 定义实验标签
tag = 'mobile_new'


# 1.留存计算
Main_Retention.main(tag)

# 2.聊天点击
Main_Chat_click_show.main(tag)

# 3.engagement 互动功能
Main_Engagement.main(tag)

# 4.business 商业化
Main_business.main(tag)

# 5.充值计算
recharge.main(tag)
recharge_summury.main(tag)

# 6.subscribe 订阅
subscribe.get_daily_subscribe_metrics_with_subscribe_rate(tag)
sub.get_and_save_daily_order_rate_by_experiment(tag)
first_new_sub.get_and_save_first_subscribe_rate_by_experiment(tag)

# 7.advertisement.main(tag)
# advertisement_sum.main(tag)














