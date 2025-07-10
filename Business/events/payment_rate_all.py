import logging
import os
import urllib.parse
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from datetime import timedelta

from growthbook_fetcher.experiment_tag_all_parameters import get_experiment_details_by_tag

import warnings
warnings.filterwarnings("ignore", category=FutureWarning)
load_dotenv()

def get_db_connection():
    password = urllib.parse.quote_plus(os.environ['DB_PASSWORD'])
    DATABASE_URL = f"mysql+pymysql://bigdata:{password}@3.135.224.186:9030/flow_ab_test?charset=utf8mb4"
    engine = create_engine(DATABASE_URL)
    logging.info("✅ 数据库连接已建立。")
    return engine

def insert_payment_ratio_data(tag, event_date, experiment_name, engine, table_name, truncate=False):
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        event_date DATE,
        country VARCHAR(64),
        variation_id VARCHAR(255),
        active_users INT,
        paying_users INT,
        purchase_rate DOUBLE
    );
    """
    with engine.connect() as conn:
        conn.execute(text("SET query_timeout = 30000;"))
        conn.execute(text(create_table_query))
        if truncate:
            conn.execute(text(f"TRUNCATE TABLE {table_name};"))
            print(f"✅ 目标表 {table_name} 已创建并清空数据。")
        insert_query = f"""
        INSERT INTO {table_name} (event_date, country, variation_id, active_users, paying_users, purchase_rate)
WITH
exp AS (
    SELECT user_id, variation_id, event_date
    FROM (
        SELECT
            user_id,
            variation_id,
            event_date,
            ROW_NUMBER() OVER (PARTITION BY user_id, event_date ORDER BY event_date DESC) AS rn
        FROM flow_wide_info.tbl_wide_experiment_assignment_hi
        WHERE experiment_id = '{experiment_name}'
          AND event_date = '{event_date}'
    ) t
    WHERE rn = 1
),
session AS (
    -- 活跃用户会话表，带国家
    SELECT s.event_date, s.user_id, geo.country
    FROM flow_event_info.tbl_app_session_info s
    JOIN flow_event_info.tbl_wide_user_active_geo_daily geo
      ON s.user_id = geo.user_id AND s.event_date = geo.event_date
    WHERE s.event_date = '{event_date}'
),
exp_active AS (
    -- 分组后活跃用户
    SELECT
        e.event_date,
        e.variation_id,
        s.country,
        e.user_id
    FROM exp e
    JOIN session s
      ON e.user_id = s.user_id AND e.event_date = s.event_date
),
pay_users AS (
    -- 当天发生购买的用户
    SELECT event_date, user_id
    FROM flow_event_info.tbl_app_event_all_purchase
    WHERE event_date = '{event_date}'
      AND type IN ('subscription', 'currency')
),
exp_pay AS (
    -- 付费活跃用户（保留国家字段）
    SELECT
        ea.event_date,
        ea.variation_id,
        ea.country,
        ea.user_id
    FROM exp_active ea
    JOIN pay_users pu
      ON ea.user_id = pu.user_id AND ea.event_date = pu.event_date
)
SELECT
    ea.event_date,
    ea.country,
    ea.variation_id,
    COUNT(DISTINCT ea.user_id) AS active_users,
    COUNT(DISTINCT ep.user_id) AS paying_users,
    ROUND(COUNT(DISTINCT ep.user_id) / NULLIF(COUNT(DISTINCT ea.user_id), 0), 4) AS purchase_rate
FROM exp_active ea
LEFT JOIN exp_pay ep
    ON ea.user_id = ep.user_id AND ea.variation_id = ep.variation_id AND ea.country = ep.country AND ea.event_date = ep.event_date
GROUP BY ea.event_date, ea.country, ea.variation_id
ORDER BY ea.event_date DESC, active_users DESC;
        """
        conn.execute(text(insert_query))
        print(f"✅ 数据已插入：{event_date}")

def daterange(start_date, end_date):
    for n in range((end_date - start_date).days + 1):
        yield start_date + timedelta(n)

def main(tag):
    print("🚀 主流程开始执行。")
    experiment_data = get_experiment_details_by_tag(tag)
    if not experiment_data:
        print(f"⚠️ 没有找到符合标签 '{tag}' 的实验数据！")
        return

    experiment_name = experiment_data['experiment_name']
    start_time = experiment_data['phase_start_time'].date()  # datetime.date
    end_time = experiment_data['phase_end_time'].date()
    table_name = f"tbl_report_payment_rate_all_{tag}"

    engine = get_db_connection()
    truncate = True
    for d in daterange(start_time + timedelta(days=1), end_time):
        insert_payment_ratio_data(
            tag=tag,
            event_date=d.strftime("%Y-%m-%d"),
            experiment_name=experiment_name,
            engine=engine,
            table_name=table_name,
            truncate=truncate
        )
        truncate = False  # 只在首次循环清空
    print("🚀 所有日期数据写入完毕。")

if __name__ == "__main__":
    main("mobile_new")
