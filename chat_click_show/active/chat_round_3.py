import urllib.parse
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
import warnings
from datetime import datetime, timedelta
import sys

from growthbook_fetcher.experiment_tag_all_parameters import get_experiment_details_by_tag

warnings.filterwarnings("ignore", category=FutureWarning)

import logging
import os
from dotenv import load_dotenv
load_dotenv()
def get_db_connection():
    password = urllib.parse.quote_plus(os.environ['DB_PASSWORD'])
    DATABASE_URL = f"mysql+pymysql://bigdata:{password}@3.135.224.186:9030/flow_ab_test?charset=utf8mb4"
    engine = create_engine(DATABASE_URL)
    logging.info("✅ 数据库连接已建立。")
    return engine

def main(tag):
    print(f"🚀 开始获取实验数据，标签：{tag}")

    experiment_data = get_experiment_details_by_tag(tag)
    if not experiment_data:
        print(f"⚠️ 没有找到符合标签 '{tag}' 的实验数据！")
        return

    experiment_name = experiment_data['experiment_name']
    start_time = experiment_data['phase_start_time']
    end_time   = experiment_data['phase_end_time']

    start_day_str = start_time.strftime("%Y-%m-%d")
    end_day_str   = end_time.strftime("%Y-%m-%d")

    engine = get_db_connection()
    table_name = f"tbl_report_chat_depth_{tag}"

    drop_table_query = f"DROP TABLE IF EXISTS {table_name};"
    create_table_query = f"""
    CREATE TABLE {table_name} (
        event_date VARCHAR(255),
        variation VARCHAR(255),
        total_chat_rounds BIGINT,
        unique_users INT,
        chat_bots INT,
        chat_depth_bot DOUBLE,
        chat_depth_user DOUBLE,
        chat_depth_per_user_per_bot DOUBLE,
        chat_depth_bot_new DOUBLE,
        chat_depth_user_new DOUBLE,
        chat_depth_per_user_per_bot_new DOUBLE,
        experiment_name VARCHAR(255)
    );
    """

    with engine.connect() as conn:
        conn.execute(text("SET query_timeout = 30000;"))
        conn.execute(text(drop_table_query))
        conn.execute(text(create_table_query))
        print(f"✅ 表 {table_name} 已创建。")

        start_date = datetime.strptime(start_day_str, "%Y-%m-%d")
        end_date = datetime.strptime(end_day_str, "%Y-%m-%d")
        delta_days = (end_date - start_date).days

        for d in range(1, delta_days):  # 排除首尾
            current_date = (start_date + timedelta(days=d)).strftime("%Y-%m-%d")

            insert_sql = f"""
            INSERT INTO {table_name} (
                event_date, variation, total_chat_rounds, unique_users, chat_bots,
                chat_depth_bot, chat_depth_user, chat_depth_per_user_per_bot,
                chat_depth_bot_new, chat_depth_user_new, chat_depth_per_user_per_bot_new,
                experiment_name
            )
            WITH dedup_assignment AS (
                SELECT user_id, event_date, variation_id
                FROM (
                    SELECT *,
                        ROW_NUMBER() OVER (
                            PARTITION BY user_id, event_date, experiment_id
                            ORDER BY variation_id
                        ) AS rn
                    FROM flow_wide_info.tbl_wide_experiment_assignment_hi
                    WHERE experiment_id = '{experiment_name}'
                ) t
                WHERE rn = 1
            ),
            first_visit_user AS (
                SELECT user_id, DATE(first_visit_date) AS first_visit_date
                FROM flow_wide_info.tbl_wide_user_first_visit_app_info
            ),
            chat_data AS (
                SELECT
                    cs.*,
                    a.variation_id,
                    u.user_id AS new_user_flag
                FROM flow_event_info.tbl_app_event_chat_send cs
                JOIN dedup_assignment a
                  ON cs.user_id = a.user_id AND cs.event_date = a.event_date
                LEFT JOIN first_visit_user u
                  ON cs.user_id = u.user_id AND cs.event_date = u.first_visit_date
                WHERE cs.event_date = '{current_date}'
                  and cs.method != 'generate'
            )
            SELECT
                '{current_date}' AS event_date,
                variation_id AS variation,
                COUNT(distinct event_id) AS total_chat_rounds,
                COUNT(DISTINCT user_id) AS unique_users,
                COUNT(DISTINCT prompt_id) AS chat_bots,
                ROUND(COUNT(distinct  event_id) * 1.0 / COUNT(DISTINCT prompt_id), 2) AS chat_depth_bot,
                ROUND(COUNT(distinct event_id) * 1.0 / COUNT(DISTINCT user_id), 2) AS chat_depth_user,
                ROUND(COUNT(distinct event_id) * 1.0 / (COUNT(DISTINCT user_id) * COUNT(DISTINCT prompt_id)), 4) AS chat_depth_per_user_per_bot,
                ROUND(SUM(CASE WHEN new_user_flag IS NOT NULL THEN 1 ELSE 0 END) * 1.0 / COUNT(DISTINCT prompt_id), 2) AS chat_depth_bot_new,
                ROUND(SUM(CASE WHEN new_user_flag IS NOT NULL THEN 1 ELSE 0 END) * 1.0 / COUNT(DISTINCT CASE WHEN new_user_flag IS NOT NULL THEN user_id END), 2) AS chat_depth_user_new,
                ROUND(SUM(CASE WHEN new_user_flag IS NOT NULL THEN 1 ELSE 0 END) * 1.0 / (
                    COUNT(DISTINCT prompt_id) * COUNT(DISTINCT CASE WHEN new_user_flag IS NOT NULL THEN user_id END)
                ), 4) AS chat_depth_per_user_per_bot_new,
                '{experiment_name}' AS experiment_name
            FROM chat_data
            GROUP BY variation_id;
            """
            print(f"👉 正在插入日期：{current_date}")
            try:
                conn.execute(text(insert_sql))
            except Exception as e:
                print(f"❌ 插入 {current_date} 失败：{e}")
                print(f"🔍 SQL:\n{insert_sql}")

        print(f"✅ 所有聊天深度数据已插入表 {table_name}。")

    result_df = pd.read_sql(f"SELECT * FROM {table_name} ORDER BY event_date, variation;", engine)
    result_df.fillna(0, inplace=True)
    print("🚀 聊天深度预览：")
    print(result_df)

if __name__ == "__main__":
    if len(sys.argv) > 1:
        tag = sys.argv[1]
    else:
        tag = "mobile_new"
        print(f"⚠️ 未指定实验标签，默认使用：{tag}")
    main(tag)
