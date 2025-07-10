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

def create_report_table(table_name, engine, truncate=False):
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        event_date DATE DEFAULT NULL,
        variation_id VARCHAR(64) DEFAULT NULL,
        total_revenue DOUBLE,
        total_order_cnt INT,
        aov DOUBLE
    );
    """
    with engine.connect() as conn:
        conn.execute(text("SET query_timeout = 30000;"))
        conn.execute(text(create_table_query))
        if truncate:
            conn.execute(text(f"TRUNCATE TABLE {table_name};"))
            logging.info(f"✅ 目标表 {table_name} 已创建并清空数据。")

def insert_payment_ratio_data(tag, event_date, experiment_name, engine, table_name):
    insert_query = f"""
    INSERT INTO {table_name} (event_date, variation_id, total_revenue, total_order_cnt, aov)
    WITH experiment_users AS (
      SELECT
        user_id,
        CAST(variation_id AS CHAR) AS variation_id
      FROM (
        SELECT
          user_id,
          variation_id,
          ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY timestamp_assigned ASC) AS rn
        FROM flow_wide_info.tbl_wide_experiment_assignment_hi
        WHERE experiment_id = '{experiment_name}'
      ) t
      WHERE rn = 1
    ),
    all_orders AS (
      SELECT 
        user_id,
        event_date,
        revenue
      FROM flow_event_info.tbl_app_event_currency_purchase
      WHERE event_date = '{event_date}'
      UNION ALL
      SELECT 
        user_id,
        event_date,
        revenue
      FROM flow_event_info.tbl_app_event_subscribe
      WHERE event_date = '{event_date}'
    ),
    orders_with_variation AS (
      SELECT 
        o.event_date,
        eu.variation_id,
        o.revenue
      FROM all_orders o
      JOIN experiment_users eu ON o.user_id = eu.user_id
    )
    SELECT
      event_date,
      variation_id,
      SUM(revenue) AS total_revenue,
      COUNT(*) AS total_order_cnt,
      ROUND(SUM(revenue) * 1.0 / NULLIF(COUNT(*), 0), 2) AS aov
    FROM orders_with_variation
    GROUP BY event_date, variation_id
    ORDER BY event_date DESC, variation_id;
    """
    try:
        with engine.connect() as conn:
            conn.execute(text(insert_query))
        logging.info(f"✅ 数据已插入：{event_date}")
    except Exception as e:
        logging.error(f"❌ 插入 {event_date} 数据失败: {e}")

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
    table_name = f"tbl_report_AOV_{tag}"

    engine = get_db_connection()
    create_report_table(table_name, engine, truncate=True)
    for d in daterange(start_time + timedelta(days=1), end_time):
        day_str = d.strftime("%Y-%m-%d")
        print(f"▶️ 正在处理 {day_str} ...")
        logging.info(f"▶️ 正在处理 {day_str} ...")
        insert_payment_ratio_data(
            tag=tag,
            event_date=day_str,
            experiment_name=experiment_name,
            engine=engine,
            table_name=table_name
        )
    print("🚀 所有日期数据写入完毕。")

if __name__ == "__main__":
    main("subscription_pricing_area")
