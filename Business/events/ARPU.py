import logging
import os
import urllib.parse
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
import warnings
from datetime import datetime


from growthbook_fetcher.experiment_tag_all_parameters import get_experiment_details_by_tag
from growthbook_fetcher.growthbook_data_ETL import fetch_and_save_experiment_data

warnings.filterwarnings("ignore", category=FutureWarning)

load_dotenv()  # ② 新增，自动读取 .env
fetch_and_save_experiment_data()

def get_db_connection():
    password = urllib.parse.quote_plus(os.environ['DB_PASSWORD'])
    DATABASE_URL = f"mysql+pymysql://bigdata:{password}@3.135.224.186:9030/flow_ab_test?charset=utf8mb4"
    engine = create_engine(DATABASE_URL)
    logging.info("✅ 数据库连接已建立。")
    return engine

def insert_arpu_data(tag):
    print(f"🚀 开始获取实验数据，标签：{tag}")
    experiment_data = get_experiment_details_by_tag(tag)
    if not experiment_data:
        print(f"⚠️ 没有找到符合标签 '{tag}' 的实验数据！")
        return None

    experiment_name = experiment_data['experiment_name']
    start_date = experiment_data['phase_start_time'].date()
    end_date = experiment_data['phase_end_time'].date()
    print(f"📝 实验名称：{experiment_name}，实验时间：{start_date} 至 {end_date}")

    engine = get_db_connection()
    table_name = f"tbl_report_arpu_{tag}"

    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        event_date DATE,
        variation_id VARCHAR(255),
        active_users INT,
        total_revenue DOUBLE,
        ARPU DOUBLE,
        experiment_tag VARCHAR(255)
    );
    """
    truncate_query = f"TRUNCATE TABLE {table_name};"

    with engine.connect() as conn:
        conn.execute(text("SET query_timeout = 30000;"))
        conn.execute(text(create_table_query))
        conn.execute(text(truncate_query))
        print(f"✅ 目标表 {table_name} 已创建并清空数据。")

        insert_query = f"""
        INSERT INTO {table_name} (event_date, variation_id, active_users, total_revenue, ARPU, experiment_tag)
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
                        AND event_date BETWEEN '{start_date}' AND '{end_date}'
                ) t
                WHERE rn = 1
            ),
            daily_active AS (
                SELECT
                    e.event_date,
                    e.variation_id,
                    COUNT(DISTINCT pv.user_id) AS active_users
                FROM flow_event_info.tbl_app_event_page_view pv
                JOIN exp e ON pv.user_id = e.user_id AND pv.event_date = e.event_date
                GROUP BY e.event_date, e.variation_id
            ),
            sub AS (
                SELECT user_id, event_date, SUM(revenue) AS sub_revenue
                FROM flow_event_info.tbl_app_event_subscribe
                WHERE event_date BETWEEN '{start_date}' AND '{end_date}'
                GROUP BY user_id, event_date
            ),
            ord AS (
                SELECT user_id, event_date, SUM(revenue) AS order_revenue
                FROM flow_event_info.tbl_app_event_currency_purchase
                WHERE event_date BETWEEN '{start_date}' AND '{end_date}'
                GROUP BY user_id, event_date
            ),
            user_revenue AS (
                SELECT
                    e.event_date,
                    e.variation_id,
                    COALESCE(s.sub_revenue, 0) + COALESCE(o.order_revenue, 0) AS total_revenue
                FROM exp e
                LEFT JOIN sub s ON e.user_id = s.user_id AND e.event_date = s.event_date
                LEFT JOIN ord o ON e.user_id = o.user_id AND e.event_date = o.event_date
            ),
            group_revenue AS (
                SELECT
                    event_date,
                    variation_id,
                    SUM(total_revenue) AS revenue
                FROM user_revenue
                GROUP BY event_date, variation_id
            ),
            -- 广告收入（每日总额，无法归属到组或用户）
            daily_ad AS (
                SELECT event_date, SUM(ad_revenue) AS ad_revenue
                FROM flow_event_info.tbl_app_event_ads_impression
                WHERE event_date BETWEEN '{start_date}' AND '{end_date}'
                GROUP BY event_date
            ),
            -- 每日总活跃用户（用于广告分摊）
            daily_total_active AS (
                SELECT event_date, SUM(active_users) AS total_active
                FROM daily_active
                GROUP BY event_date
            )
        SELECT
            da.event_date,
            da.variation_id,
            da.active_users,
            -- 每组总收入 = 组充值+订阅收入 + 按活跃用户占比分摊广告收入
            COALESCE(gr.revenue, 0)
                + COALESCE(dad.ad_revenue, 0) * da.active_users / NULLIF(dta.total_active, 0)
                AS total_revenue,
            ROUND(
                (
                    COALESCE(gr.revenue, 0)
                    + COALESCE(dad.ad_revenue, 0) * da.active_users / NULLIF(dta.total_active, 0)
                ) / NULLIF(da.active_users, 0),
            4) AS ARPU,
            '{tag}' AS experiment_tag
        FROM daily_active da
        LEFT JOIN group_revenue gr
            ON da.event_date = gr.event_date AND da.variation_id = gr.variation_id
        LEFT JOIN daily_ad dad
            ON da.event_date = dad.event_date
        LEFT JOIN daily_total_active dta
            ON da.event_date = dta.event_date
        WHERE da.event_date > '{start_date}' AND da.event_date < '{end_date}';
        """
        conn.execute(text(insert_query))
        print(f"✅ ARPU 明细数据已插入到表 {table_name}")
    return table_name




def main(tag):
    print("🚀 主流程开始执行。")
    table_name = insert_arpu_data(tag)
    if table_name is None:
        print("⚠️ 数据写入或建表失败！")
        return
    print("🚀 主流程执行完毕。")

if __name__ == "__main__":
    main("subscription_pricing_area")
