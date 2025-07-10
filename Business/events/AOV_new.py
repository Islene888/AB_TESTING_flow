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
        event_date DATE,
        variation_id VARCHAR(64),
        revenue_day1 DOUBLE,
        order_cnt_day1 INT,
        aov_day1 DOUBLE,
        revenue_day3 DOUBLE,
        order_cnt_day3 INT,
        aov_day3 DOUBLE
    )
    ENGINE=OLAP
    DUPLICATE KEY(event_date, variation_id)
    DISTRIBUTED BY HASH(event_date)
    PROPERTIES (
        "replication_num" = "1"
    );
    """
    with engine.connect() as conn:
        conn.execute(text("SET query_timeout = 30000;"))
        conn.execute(text(create_table_query))
        if truncate:
            conn.execute(text(f"TRUNCATE TABLE {table_name};"))
            logging.info(f"✅ 目标表 {table_name} 已创建并清空数据。")

def insert_newuser_aov_data(tag, event_date_str, experiment_name, engine, table_name):
    insert_query = f"""
    INSERT INTO {table_name} (
        event_date, variation_id,
        revenue_day1, order_cnt_day1, aov_day1,
        revenue_day3, order_cnt_day3, aov_day3
    )
    WITH
    new_users AS (
        SELECT user_id
        FROM flow_wide_info.tbl_wide_user_first_visit_app_info
        WHERE DATE(first_visit_date) = '{event_date_str}'
    ),
    experiment_users AS (
        SELECT t.user_id, t.variation_id
        FROM (
            SELECT
                user_id,
                variation_id,
                event_date,
                ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY event_date DESC) AS rn
            FROM flow_wide_info.tbl_wide_experiment_assignment_hi
            WHERE experiment_id = '{experiment_name}'
              AND event_date = '{event_date_str}'
        ) t
        WHERE rn = 1
    ),
    new_exp_users AS (
        SELECT e.user_id, e.variation_id
        FROM experiment_users e
        INNER JOIN new_users n ON e.user_id = n.user_id
    ),
    orders_with_new_users AS (
        SELECT
            n.variation_id,
            o.event_date,
            o.revenue
        FROM new_exp_users n
        JOIN (
            SELECT user_id, event_date, revenue FROM flow_event_info.tbl_app_event_subscribe
            UNION ALL
            SELECT user_id, event_date, revenue FROM flow_event_info.tbl_app_event_currency_purchase
        ) o ON n.user_id = o.user_id
        WHERE o.event_date >= '{event_date_str}'
          AND o.event_date <= DATE_ADD('{event_date_str}', INTERVAL 3 DAY)
    )
    SELECT
      '{event_date_str}' AS event_date,
      variation_id,
      -- day1
      SUM(CASE WHEN event_date <= DATE_ADD('{event_date_str}', INTERVAL 1 DAY) THEN revenue ELSE 0 END) AS revenue_day1,
      COUNT(CASE WHEN event_date <= DATE_ADD('{event_date_str}', INTERVAL 1 DAY) THEN 1 END) AS order_cnt_day1,
      ROUND(
        SUM(CASE WHEN event_date <= DATE_ADD('{event_date_str}', INTERVAL 1 DAY) THEN revenue ELSE 0 END) 
        / NULLIF(COUNT(CASE WHEN event_date <= DATE_ADD('{event_date_str}', INTERVAL 1 DAY) THEN 1 END), 0), 2
      ) AS aov_day1,
      -- day3
      SUM(revenue) AS revenue_day3,
      COUNT(*) AS order_cnt_day3,
      ROUND(SUM(revenue) / NULLIF(COUNT(*), 0), 2) AS aov_day3
    FROM orders_with_new_users
    GROUP BY variation_id;
    """
    try:
        with engine.connect() as conn:
            conn.execute(text(insert_query))
        logging.info(f"✅ 新用户 {event_date_str} day1/day3 AOV 数据已插入。")
    except Exception as e:
        logging.error(f"❌ 插入新用户 {event_date_str} day1/day3 AOV 数据失败: {e}")

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
    table_name = f"tbl_report_newuser_aov_{tag}"

    engine = get_db_connection()
    create_report_table(table_name, engine, truncate=True)
    for d in daterange(start_time, end_time):
        day_str = d.strftime("%Y-%m-%d")
        print(f"▶️ 正在处理新用户 {day_str} ...")
        logging.info(f"▶️ 正在处理新用户 {day_str} ...")
        insert_newuser_aov_data(
            tag=tag,
            event_date_str=day_str,
            experiment_name=experiment_name,
            engine=engine,
            table_name=table_name
        )
    print("🚀 新用户AOV统计写入完毕。")

if __name__ == "__main__":
    main("new_ui")
