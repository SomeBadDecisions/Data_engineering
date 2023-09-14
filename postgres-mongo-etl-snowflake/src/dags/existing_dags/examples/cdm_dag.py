import pendulum
from datetime import datetime, timedelta 
from airflow import DAG 
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import logging
import psycopg2

default_args = {
    'owner': 'Airflow',
    'retries': 0
}

target_hook = PostgresHook(postgres_conn_id="PG_WAREHOUSE_CONNECTION")

def cdm_load():
 
    
    
    with target_hook.get_conn() as conn:
        conn.autocommit = False
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
 
        cursor.execute("""
                        WITH order_sums AS (
    SELECT
        r.restaurant_id                 AS restaurant_id,
        r.restaurant_name               AS restaurant_name,
        tss.date                        AS settlement_date,
        COUNT(DISTINCT fct.order_id)    AS orders_count,
        SUM(fct.total_sum)              AS orders_total_sum,
        SUM(fct.bonus_payment)          AS orders_bonus_payment_sum,
        SUM(fct.bonus_grant)            AS orders_bonus_granted_sum
    FROM dds.fct_product_sales as fct
        INNER JOIN dds.dm_orders AS orders
            ON fct.order_id = orders.id
        INNER JOIN dds.dm_timestamps as tss
            ON tss.id = orders.timestamp_id
        INNER JOIN dds.dm_restaurants AS r
            on r.id = orders.restaurant_id
    WHERE orders.order_status = 'CLOSED'
    GROUP BY
        r.restaurant_id,
        r.restaurant_name,
        tss.date
)
INSERT INTO cdm.dm_settlement_report(
    restaurant_id,
    restaurant_name,
    settlement_date,
    orders_count,
    orders_total_sum,
    orders_bonus_payment_sum,
    orders_bonus_granted_sum,
    order_processing_fee,
    restaurant_reward_sum
)
SELECT
    restaurant_id,
    restaurant_name,
    settlement_date,
    orders_count,
    orders_total_sum,
    orders_bonus_payment_sum,
    orders_bonus_granted_sum,
    s.orders_total_sum * 0.25 AS order_processing_fee,
    s.orders_total_sum - s.orders_total_sum * 0.25 - s.orders_bonus_payment_sum AS restaurant_reward_sum
FROM order_sums AS s
ON CONFLICT (restaurant_id, settlement_date) DO UPDATE
SET
    orders_count = EXCLUDED.orders_count,
    orders_total_sum = EXCLUDED.orders_total_sum,
    orders_bonus_payment_sum = EXCLUDED.orders_bonus_payment_sum,
    orders_bonus_granted_sum = EXCLUDED.orders_bonus_granted_sum,
    order_processing_fee = EXCLUDED.order_processing_fee,
    restaurant_reward_sum = EXCLUDED.restaurant_reward_sum;
                        ;""")
 
        conn.commit()


with DAG(
    dag_id="cdm_dag",
    default_args=default_args,
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    schedule_interval='0/15 * * * *',
    catchup=False,
    tags=['sprint5']
) as dag:


    final= PythonOperator (
        task_id="the_only_task",
        python_callable=cdm_load
    )


    final 