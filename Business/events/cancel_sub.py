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

def insert_unsub_rate_data(tag, event_date, experiment_name, engine, table_name, truncate=False):
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        event_date DATE,
        country VARCHAR(64),
        variation_id VARCHAR(255),
        total_subs INT,
        unsub_in3d INT,
        unsub_rate_day3 DOUBLE
    );
    """
    with engine.connect() as conn:
        conn.execute(text("SET query_timeout = 30000;"))
        conn.execute(text(create_table_query))
        if truncate:
            conn.execute(text(f"TRUNCATE TABLE {table_name};"))
            print(f"✅ 目标表 {table_name} 已创建并清空数据。")
        insert_query = f"""
        INSERT INTO {table_name} (event_date, country, variation_id, total_subs, unsub_in3d, unsub_rate_day3)
WITH union_events AS (
    SELECT user_id, order_id, country, DATE(sub_date) AS sub_date, notification_type, 'apple' AS store_type
    FROM flow_wide_info.tbl_wide_business_subscribe_apple_detail
    UNION ALL
    SELECT user_id, order_id, country, DATE(sub_date) AS sub_date, notification_type, 'google' AS store_type
    FROM flow_wide_info.tbl_wide_business_subscribe_google_detail
),
exp_group AS (
  SELECT user_id, event_date, variation_id
  FROM (
    SELECT
      user_id,
      event_date,
      variation_id,
      ROW_NUMBER() OVER (PARTITION BY user_id, event_date ORDER BY event_date DESC) AS rn
    FROM flow_wide_info.tbl_wide_experiment_assignment_hi
    WHERE experiment_id = '{experiment_name}'
  ) t
  WHERE rn = 1
),
active_users AS (
    SELECT 
        a.event_date,
        a.user_id,
        a.country
    FROM flow_event_info.tbl_wide_user_active_geo_daily a
    WHERE a.event_date = '{event_date}'
),
new_subs AS (
  SELECT
    e.user_id,
    e.order_id,
    au.country,
    e.sub_date,
    e.store_type,
    g.variation_id
  FROM union_events e
  LEFT JOIN exp_group g
    ON e.user_id = g.user_id AND e.sub_date = g.event_date
  LEFT JOIN active_users au
    ON e.user_id = au.user_id AND e.sub_date = au.event_date
  WHERE 
    (
      (e.store_type = 'apple' AND e.notification_type IN ('SUBSCRIBED', 'DID_RENEW'))
      OR
      (e.store_type = 'google' AND e.notification_type IN ('2', '4'))
    )
    AND e.sub_date = '{event_date}'
),
cancel AS (
  SELECT 
    c.user_id, 
    c.order_id, 
    au.country AS country, 
    c.sub_date AS cancel_date, 
    c.store_type
  FROM union_events c
  LEFT JOIN active_users au
    ON c.user_id = au.user_id AND c.sub_date = au.event_date
  WHERE 
    (
      (c.store_type = 'apple' AND c.notification_type IN ('DID_CHANGE_RENEWAL_STATUS'))
      OR
      (c.store_type = 'google' AND c.notification_type = '3')
    )
)
SELECT
  '{event_date}' AS event_date,
  n.country,
  n.variation_id,
  COUNT(DISTINCT n.user_id) AS total_subs,
  COUNT(DISTINCT CASE WHEN c.cancel_date IS NOT NULL AND c.cancel_date <= DATE_ADD(n.sub_date, INTERVAL 3 DAY) THEN n.user_id END) AS unsub_in3d,
  ROUND(
    COUNT(DISTINCT CASE WHEN c.cancel_date IS NOT NULL AND c.cancel_date <= DATE_ADD(n.sub_date, INTERVAL 3 DAY) THEN n.user_id END)
    / NULLIF(COUNT(DISTINCT n.user_id), 0), 4
  ) AS unsub_rate_day3
FROM new_subs n
LEFT JOIN cancel c
  ON n.user_id = c.user_id AND n.order_id = c.order_id AND n.country = c.country
    AND c.cancel_date >= n.sub_date AND c.cancel_date <= DATE_ADD(n.sub_date, INTERVAL 3 DAY)
WHERE n.variation_id IS NOT NULL
GROUP BY n.country, n.variation_id
ORDER BY n.country, n.variation_id
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
    table_name = f"tbl_report_unsub_rate_day3_{tag}"

    engine = get_db_connection()
    truncate = True
    for d in daterange(start_time, end_time):
        insert_unsub_rate_data(
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
    main("subscription_pricing_area")
