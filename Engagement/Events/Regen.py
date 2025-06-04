import urllib.parse
import pandas as pd
from sqlalchemy import create_engine, text
import warnings
from datetime import datetime, timedelta
import sys
import time

from state2.growthbook_fetcher.experiment_tag_all_parameters import get_experiment_details_by_tag

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

def insert_data_by_variation_batch(conn, table_name, experiment_name, current_date, variations, batch_size=2):
    for i in range(0, len(variations), batch_size):
        batch = variations[i:i+batch_size]
        for var in batch:
            insert_query = f"""
            INSERT INTO {table_name} (event_date, variation, total_regen, unique_regen_users, regen_ratio, experiment_name)
            SELECT
                '{current_date}' AS event_date,
                '{var}' AS variation,
                COUNT(DISTINCT a.event_id) AS total_regen,
                COUNT(DISTINCT a.user_id) AS unique_regen_users,
                CASE
                    WHEN COUNT(DISTINCT a.user_id) = 0 THEN 0
                    ELSE ROUND(COUNT(DISTINCT a.event_id) * 1.0 / COUNT(DISTINCT a.user_id), 4)
                END AS regen_ratio,
                '{experiment_name}' AS experiment_name
            FROM flow_event_info.tbl_app_event_chat_send a
            JOIN flow_wide_info.tbl_wide_experiment_assignment_hi b
                ON a.user_id = b.user_id
            WHERE b.experiment_id = '{experiment_name}'
              AND a.event_date = '{current_date}'
              AND a.Method = 'regenerate'
              AND b.variation_id = '{var}'
            """
            conn.execute(text(insert_query))
        print(f"✅ 已插入日期 {current_date} 的变体批次：{batch}")
        time.sleep(0.5)

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
    start_time_str = start_time.strftime("%Y-%m-%d %H:%M:%S")
    end_time_str   = end_time.strftime("%Y-%m-%d %H:%M:%S")

    print(f"📝 实验名称：{experiment_name}")
    print(f"⏰ 计算时间范围：{start_time_str} ~ {end_time_str}")
    print(f"   首日：{start_day_str}，末日：{end_day_str}")

    engine = get_db_connection()
    table_name = f"tbl_report_regen_{tag}"

    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        event_date VARCHAR(255),
        variation VARCHAR(255),
        total_regen INT,
        unique_regen_users INT,
        regen_ratio DOUBLE,
        experiment_name VARCHAR(255)
    );
    """
    truncate_query = f"TRUNCATE TABLE {table_name};"

    with engine.connect() as conn:
        conn.execute(text("SET query_timeout = 30000;"))
        conn.execute(text(create_table_query))
        conn.execute(text(truncate_query))
        print(f"✅ 表 {table_name} 已创建并清空。")

        # 获取所有变体 ID
        variation_query = f"SELECT DISTINCT variation_id FROM flow_wide_info.tbl_wide_experiment_assignment_hi WHERE experiment_id = '{experiment_name}'"
        variation_result = conn.execute(text(variation_query)).fetchall()
        variations = [row[0] for row in variation_result]

        # 逐日插入（跳过首尾）
        start_date = datetime.strptime(start_day_str, "%Y-%m-%d")
        end_date = datetime.strptime(end_day_str, "%Y-%m-%d")
        delta_days = (end_date - start_date).days

        for d in range(1, delta_days):
            current_date = (start_date + timedelta(days=d)).strftime("%Y-%m-%d")
            print(f"👉 正在插入日期：{current_date}")
            insert_data_by_variation_batch(conn, table_name, experiment_name, current_date, variations, batch_size=2)

        print(f"✅ 所有按天 regen 数据已成功插入到表 {table_name} 中。")

    result_df = pd.read_sql(f"SELECT * FROM {table_name} ORDER BY event_date, variation;", engine)
    print("🚀 最终表数据（不含首尾天）:")
    print(result_df)

if __name__ == "__main__":
    if len(sys.argv) > 1:
        tag = sys.argv[1]
    else:
        tag = "chat_0521"
        print(f"⚠️ 未指定实验标签，默认使用：{tag}")
    main(tag)
