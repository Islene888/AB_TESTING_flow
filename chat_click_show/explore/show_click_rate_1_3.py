import sys
import urllib.parse
import pandas as pd
from sqlalchemy import create_engine, text
import warnings
from datetime import datetime, timedelta
from growthbook_fetcher.experiment_tag_all_parameters import get_experiment_details_by_tag
import logging
import os
from dotenv import load_dotenv

warnings.filterwarnings("ignore", category=FutureWarning)
load_dotenv()

def get_db_connection():
    password = urllib.parse.quote_plus(os.environ['DB_PASSWORD'])
    DATABASE_URL = f"mysql+pymysql://bigdata:{password}@3.135.224.186:9030/flow_ab_test?charset=utf8mb4"
    engine = create_engine(DATABASE_URL)
    logging.info("✅ 数据库连接已建立。")
    return engine

def main(tag):
    print(f"🚀 开始插入点击指标数据，标签：{tag}")

    experiment_data = get_experiment_details_by_tag(tag)
    if not experiment_data:
        print(f"⚠️ 没有找到符合标签 '{tag}' 的实验数据！")
        return

    experiment_name = experiment_data['experiment_name']
    start_time = experiment_data['phase_start_time']
    end_time = experiment_data['phase_end_time']

    start_date = datetime.strptime(start_time.strftime("%Y-%m-%d"), "%Y-%m-%d")
    end_date = datetime.strptime(end_time.strftime("%Y-%m-%d"), "%Y-%m-%d")
    delta_days = (end_date - start_date).days

    engine = get_db_connection()
    table_name = f"tbl_report_user_clickrate_{tag}"

    drop_table_sql = f"DROP TABLE IF EXISTS {table_name};"
    create_table_sql = f"""
        CREATE TABLE {table_name} (
            event_date STRING,
            variation STRING,
            show_users BIGINT,
            avg_shows_per_user DOUBLE,
            click_rate DOUBLE,
            new_user_show_users BIGINT,
            avg_shows_per_new_user DOUBLE,
            click_rate_new_user DOUBLE,
            experiment_name STRING
        );
    """

    with engine.connect() as conn:
        conn.execute(text("SET query_timeout = 30000;"))
        conn.execute(text(drop_table_sql))
        conn.execute(text(create_table_sql))
        print(f"✅ 表 {table_name} 已创建。")

        for d in range(1, delta_days-1):  # 排除首尾
            current_date = (start_date + timedelta(days=d)).strftime("%Y-%m-%d")
            print(f"👉 正在插入日期：{current_date}")

            insert_sql = f"""
            INSERT INTO {table_name}
            WITH experiment_assignment_dedup AS (
                SELECT *
                FROM (
                    SELECT
                        user_id,
                        experiment_id,
                        variation_id,
                        event_date,
                        ROW_NUMBER() OVER (PARTITION BY user_id, experiment_id ORDER BY event_date ASC) AS rn
                    FROM flow_wide_info.tbl_wide_experiment_assignment_hi
                    WHERE experiment_id = '{experiment_name}'
                ) t
                WHERE t.rn = 1
            ),
            first_visit_user AS (
                SELECT user_id, DATE(first_visit_date) AS first_visit_date
                FROM flow_wide_info.tbl_wide_user_first_visit_app_info
            ),
            show_info AS (
                SELECT user_id, COUNT(DISTINCT event_id) AS shows
                FROM flow_event_info.tbl_app_event_show_prompt_card
                WHERE event_date = '{current_date}'
                  AND current_page = 'home'
                  AND tab_name = 'Explore'
                GROUP BY user_id
            ),
            click_info AS (
                SELECT user_id, COUNT(DISTINCT event_id) AS clicks
                FROM flow_event_info.tbl_app_event_bot_view
                WHERE event_date = '{current_date}'
                  AND source = 'tag:Explore'
                GROUP BY user_id
            ),
            raw_data AS (
                SELECT
                    '{current_date}' AS event_date,
                    ea.variation_id AS variation,
                    ea.user_id AS user_id,
                    COALESCE(s.shows, 0) AS shows,
                    COALESCE(c.clicks, 0) AS clicks,
                    CASE WHEN u.user_id IS NOT NULL AND u.first_visit_date = '{current_date}' THEN 1 ELSE 0 END AS is_new_user
                FROM experiment_assignment_dedup ea
                LEFT JOIN show_info s ON ea.user_id = s.user_id
                LEFT JOIN click_info c ON ea.user_id = c.user_id
                LEFT JOIN first_visit_user u ON ea.user_id = u.user_id
            )
            SELECT
                event_date,
                variation,
                COUNT(DISTINCT user_id) AS show_users,
                ROUND(SUM(shows) * 1.0 / NULLIF(COUNT(DISTINCT user_id), 0), 4) AS avg_shows_per_user,
                ROUND(SUM(clicks) * 1.0 / NULLIF(SUM(shows), 0), 4) AS click_rate,
            
                COUNT(DISTINCT CASE WHEN is_new_user = 1 THEN user_id END) AS new_user_show_users,
                ROUND(SUM(CASE WHEN is_new_user = 1 THEN shows ELSE 0 END) * 1.0 / NULLIF(COUNT(DISTINCT CASE WHEN is_new_user = 1 THEN user_id END), 0), 4) AS avg_shows_per_new_user,
                ROUND(SUM(CASE WHEN is_new_user = 1 THEN clicks ELSE 0 END) * 1.0 / NULLIF(SUM(CASE WHEN is_new_user = 1 THEN shows ELSE 0 END), 0), 4) AS click_rate_new_user,
            
                '{experiment_name}' AS experiment_name
            FROM raw_data
            GROUP BY event_date, variation;
            """
            try:
                conn.execute(text(insert_sql))
                print(f"✅ 插入 {current_date} 成功。")
            except Exception as e:
                print(f"❌ 插入 {current_date} 失败：{e}")


if __name__ == "__main__":
    if len(sys.argv) > 1:
        tag = sys.argv[1]
    else:
        tag = "pool_rank_exp"  # 可设置默认标签
        print(f"⚠️ 未指定实验标签，默认使用：{tag}")

    main(tag)

