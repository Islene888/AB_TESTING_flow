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

def insert_newuser_payment_rate(tag, event_date, experiment_name, engine, table_name, truncate=False):
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        event_date DATE,
        variation_id VARCHAR(255),
        country VARCHAR(64),
        new_users INT,
        pay_user_day1 INT,
        pay_rate_day1 DOUBLE,
        pay_user_day3 INT,
        pay_rate_day3 DOUBLE
    );
    """
    day3_date = (event_date + timedelta(days=3)).strftime("%Y-%m-%d")
    event_date_str = event_date.strftime("%Y-%m-%d")
    with engine.connect() as conn:
        conn.execute(text("SET query_timeout = 30000;"))
        conn.execute(text(create_table_query))
        if truncate:
            conn.execute(text(f"TRUNCATE TABLE {table_name};"))
            print(f"✅ 目标表 {table_name} 已创建并清空数据。")
        insert_query = f"""
        INSERT INTO {table_name} (
            event_date, variation_id, country, new_users, pay_user_day1, pay_rate_day1, pay_user_day3, pay_rate_day3
        )
WITH exp AS (
    -- 实验分组（当天最新分组）
    SELECT user_id, variation_id, event_date
    FROM (
        SELECT
            user_id,
            variation_id,
            event_date,
            ROW_NUMBER() OVER (PARTITION BY user_id, event_date ORDER BY event_date DESC) AS rn
        FROM flow_wide_info.tbl_wide_experiment_assignment_hi
        WHERE experiment_id = '{experiment_name}'
          AND event_date = '{event_date_str}'
    ) t
    WHERE rn = 1
),
user_geo AS (
    -- 新用户当天国家
    SELECT user_id, event_date, country
    FROM flow_event_info.tbl_wide_user_active_geo_daily
    WHERE event_date = '{event_date_str}'
),
new_users AS (
    -- 新用户+实验分组+国家
    SELECT
        f.user_id,
        f.first_visit_date AS event_date,          
        COALESCE(g.country, 'unknown') AS country,
        COALESCE(e.variation_id, 'unknown') AS variation_id
    FROM flow_wide_info.tbl_wide_user_first_visit_app_info f
    LEFT JOIN user_geo g ON f.user_id = g.user_id AND f.first_visit_date = g.event_date
    LEFT JOIN exp e ON f.user_id = e.user_id AND f.first_visit_date = e.event_date
    WHERE f.first_visit_date = '{event_date_str}'
),
pay_user AS (
    -- 新用户3天内的付费明细
    SELECT
        n.user_id,
        n.event_date,
        n.country,
        n.variation_id,
        p.event_date AS pay_event_date
    FROM new_users n
    LEFT JOIN flow_event_info.tbl_app_event_all_purchase p
        ON n.user_id = p.user_id
        AND p.type IN ('subscription', 'currency')
        AND p.event_date BETWEEN '{event_date_str}' AND '{day3_date}'
)
SELECT
    '{event_date_str}' AS event_date,
    variation_id,
    country,
    COUNT(DISTINCT user_id) AS new_users,
    COUNT(DISTINCT CASE WHEN pay_event_date <= DATE_ADD('{event_date_str}', INTERVAL 1 DAY) THEN user_id END) AS pay_user_day1,
    ROUND(
        COUNT(DISTINCT CASE WHEN pay_event_date <= DATE_ADD('{event_date_str}', INTERVAL 1 DAY) THEN user_id END) / NULLIF(COUNT(DISTINCT user_id), 0),
        4
    ) AS pay_rate_day1,
    COUNT(DISTINCT CASE WHEN pay_event_date <= DATE_ADD('{event_date_str}', INTERVAL 3 DAY) THEN user_id END) AS pay_user_day3,
    ROUND(
        COUNT(DISTINCT CASE WHEN pay_event_date <= DATE_ADD('{event_date_str}', INTERVAL 3 DAY) THEN user_id END) / NULLIF(COUNT(DISTINCT user_id), 0),
        4
    ) AS pay_rate_day3
FROM pay_user
WHERE variation_id IS NOT NULL  
  AND variation_id != 'unknown'
GROUP BY variation_id, country
ORDER BY variation_id, country;
        """
        conn.execute(text(insert_query))
        print(f"✅ 数据已插入：{event_date_str}")

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
    start_time = experiment_data['phase_start_time'].date()
    end_time = experiment_data['phase_end_time'].date()
    table_name = f"tbl_report_payment_rate_new_{tag}"

    engine = get_db_connection()
    truncate = True
    for d in daterange(start_time, end_time):
        insert_newuser_payment_rate(
            tag=tag,
            event_date=d,
            experiment_name=experiment_name,
            engine=engine,
            table_name=table_name,
            truncate=truncate
        )
        truncate = False  # 只在首次循环清空
    print("🚀 所有日期数据写入完毕。")

if __name__ == "__main__":
    main("mobile_new")
