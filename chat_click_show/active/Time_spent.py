import os
import sys
import urllib.parse
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
import warnings
import logging
from datetime import datetime, timedelta
from growthbook_fetcher.experiment_tag_all_parameters import get_experiment_details_by_tag

warnings.filterwarnings("ignore", category=FutureWarning)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
load_dotenv()


def get_db_connection():
    password = urllib.parse.quote_plus(os.environ['DB_PASSWORD'])
    DATABASE_URL = f"mysql+pymysql://bigdata:{password}@3.135.224.186:9030/flow_ab_test?charset=utf8mb4"
    engine = create_engine(DATABASE_URL)
    logging.info("✅ 数据库连接已建立。")
    return engine


def insert_time_spent_data(tag):
    logging.info(f"🚀 开始获取实验数据，标签：{tag}")
    experiment_data = get_experiment_details_by_tag(tag)
    if not experiment_data:
        logging.warning(f"⚠️ 没有找到符合标签 '{tag}' 的实验数据！")
        return None

    experiment_name = experiment_data["experiment_name"]
    start_time_full = experiment_data["phase_start_time"]
    end_time_full = experiment_data["phase_end_time"]
    logging.info(f"📝 实验名称：{experiment_name}，实验时间：{start_time_full} 至 {end_time_full}")

    # 取日期部分
    start_day = start_time_full.date()
    end_day = end_time_full.date()

    engine = get_db_connection()
    table_name = f"tbl_report_time_spent_{tag}"

    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        event_date DATE,
        variation VARCHAR(255),
        total_time_minutes DOUBLE,
        unique_users INT,
        avg_time_spent_minutes DOUBLE,
        new_user_total_time_minutes DOUBLE,
        new_user_count INT,
        new_user_avg_time_spent_minutes DOUBLE,
        experiment_name VARCHAR(255)
    );
    """

    with engine.connect() as conn:
        conn.execute(text("SET query_timeout = 30000;"))
        conn.execute(text(create_table_query))
        conn.execute(text(f"TRUNCATE TABLE {table_name};"))  # 先清空旧数据

    current_day = start_day + timedelta(days=1)
    while current_day <= end_day:
        current_day_str = current_day.strftime("%Y-%m-%d")
        logging.info(f"⚡️ 正在处理日期：{current_day_str}")

        insert_query = f"""
        INSERT INTO {table_name} (
            event_date, variation, total_time_minutes, unique_users, avg_time_spent_minutes,
            new_user_total_time_minutes, new_user_count, new_user_avg_time_spent_minutes, experiment_name
        )
        WITH session_agg AS (
            SELECT
                DATE(event_date) AS event_date,                                 
                user_id,                                      
                ROUND(SUM(duration) / 1000 / 60, 2) AS total_time_minutes  
            FROM flow_event_info.tbl_app_session_info
            WHERE DATE(event_date) = '{current_day_str}'
            GROUP BY DATE(event_date), user_id
        ),
        experiment_var AS (
            SELECT user_id, variation_id
            FROM (
                SELECT
                    user_id,
                    variation_id,
                    ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY event_date) AS rn
                FROM flow_wide_info.tbl_wide_experiment_assignment_hi
                WHERE experiment_id = '{experiment_name}'
                  AND event_date = '{current_day_str}'
            ) t
            WHERE rn = 1
        ),
        new_users AS (
            SELECT user_id
            FROM flow_wide_info.tbl_wide_user_first_visit_app_info
            WHERE DATE(first_visit_date) = '{current_day_str}'
        )
        SELECT
            sa.event_date,
            ev.variation_id AS variation,
            SUM(sa.total_time_minutes) AS total_time_minutes,
            COUNT(DISTINCT sa.user_id) AS unique_users,
            ROUND(SUM(sa.total_time_minutes) / NULLIF(COUNT(DISTINCT sa.user_id), 0), 2) AS avg_time_spent_minutes,
            SUM(CASE WHEN nu.user_id IS NOT NULL THEN sa.total_time_minutes ELSE 0 END) AS new_user_total_time_minutes,
            COUNT(DISTINCT CASE WHEN nu.user_id IS NOT NULL THEN sa.user_id END) AS new_user_count,
            ROUND(
                SUM(CASE WHEN nu.user_id IS NOT NULL THEN sa.total_time_minutes ELSE 0 END) 
                / NULLIF(COUNT(DISTINCT CASE WHEN nu.user_id IS NOT NULL THEN sa.user_id END), 0), 2
            ) AS new_user_avg_time_spent_minutes,
            '{experiment_name}' AS experiment_name
        FROM session_agg sa
        JOIN experiment_var ev ON sa.user_id = ev.user_id
        LEFT JOIN new_users nu ON sa.user_id = nu.user_id
        GROUP BY sa.event_date, ev.variation_id
        ORDER BY sa.event_date, ev.variation_id;
        """

        try:
            with engine.connect() as conn:
                conn.execute(text(insert_query))
            logging.info(f"✅ 日期 {current_day_str} 数据插入完成，表名：{table_name}")
        except Exception as e:
            logging.error(f"❌ 插入日期 {current_day_str} 数据失败: {e}")

        current_day += timedelta(days=1)

    logging.info(f"✅ 所有日期的数据插入完成，表名：{table_name}")
    return table_name


def main(tag):
    logging.info("✨ 主流程开始执行。")
    table_name = insert_time_spent_data(tag)
    if table_name is None:
        logging.error("❌ 数据写入或建表失败！")
        return
    logging.info("✅ 主流程执行完毕。")


if __name__ == "__main__":
    if len(sys.argv) > 1:
        tag = sys.argv[1]
    else:
        tag = "chat_summary"
        print(f"⚠️ 未指定实验标签，默认使用：{tag}")
    main(tag)
