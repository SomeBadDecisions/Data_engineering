import requests
import json
from psycopg2.extras import execute_values
import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.base import BaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.task_group import TaskGroup

task_logger = logging.getLogger('airflow.task')

# подключение к ресурсам
#api_conn = BaseHook.get_connection('api_conn')
postgres_conn = 'PG_WAREHOUSE_CONNECTION'
dwh_hook = PostgresHook(postgres_conn)

# параметры API
nickname = 'MrK'
cohort = '6'
base_url = 'd5d04q7d963eapoepsqr.apigw.yandexcloud.net'

headers = {"X-Nickname" : nickname,
         'X-Cohort' : cohort,
         'X-API-KEY' : "25c27781-8fde-4b30-a22e-524044a7580f",
         }
         


def upload_couriers(pg_schema, pg_table):

    conn = dwh_hook.get_conn()
    cursor = conn.cursor()
    
    # идемпотентность
    dwh_hook.run(sql = f"DELETE FROM {pg_schema}.{pg_table}")

   
    offset = 0
    while True:    
        couriers_rep = requests.get(f'https://{base_url}/couriers/?sort_field=_id&sort_direction=asc&offset={offset}',
                            headers = headers).json()

        if len(couriers_rep) == 0:
            conn.commit()
            cursor.close()
            conn.close()
            task_logger.info(f'Writting {offset} rows')
            break

        
        
        values = [[value for value in couriers_rep[i].values()] for i in range(len(couriers_rep))]

        sql = f"INSERT INTO {pg_schema}.{pg_table} (id,name) VALUES %s"
        execute_values(cursor, sql, values)

        offset += len(couriers_rep)  

def upload_deliveries(pg_schema, pg_table):

    conn = dwh_hook.get_conn()
    cursor = conn.cursor()
    
    # идемпотентность
    dwh_hook.run(sql = f"DELETE FROM {pg_schema}.{pg_table}")

   
    offset = 0
    while True:    
        deliveries_rep = requests.get(f'https://{base_url}/deliveries/?sort_field=_id&sort_direction=asc&offset={offset}',
                            headers = headers).json()

        if len(deliveries_rep) == 0:
            conn.commit()
            cursor.close()
            conn.close()
            task_logger.info(f'Writting {offset} rows')
            break

        
        columns = ','.join([i for i in deliveries_rep[0]])
        values = [[value for value in deliveries_rep[i].values()] for i in range(len(deliveries_rep))]

        sql = f"INSERT INTO {pg_schema}.{pg_table} ({columns}) VALUES %s"
        execute_values(cursor, sql, values)

        offset += len(deliveries_rep)  


default_args = {
    'owner':'airflow',
    'retries':0
}


with DAG ('dwh_update',
        start_date=datetime(2022, 10, 15),
        catchup=False,
        schedule_interval='@daily',
        max_active_runs=1,
        default_args=default_args) as dag:


    upload_couriers = PythonOperator(
                task_id = 'stg_couriers',
                python_callable = upload_couriers,
                op_kwargs = {
                    'pg_schema' : 'stg',
                    'pg_table' : 'deliverysystem_couriers'
                })

    upload_deliveries = PythonOperator(
                task_id = 'stg_deliveries',
                python_callable = upload_deliveries,
                op_kwargs = {
                    'pg_schema' : 'stg',
                    'pg_table' : 'deliverysystem_deliveries'
                })

    upload_dim_couriers = PostgresOperator(
        task_id='dim_couriers',
        postgres_conn_id=postgres_conn,
        sql="sql/dim_couriers.sql")

    upload_fct_order_rates = PostgresOperator(
        task_id='fct_order_rates',
        postgres_conn_id=postgres_conn,
        sql="sql/fct_order_rates.sql")

    upload_ledger = PostgresOperator(
        task_id='ledger',
        postgres_conn_id=postgres_conn,
        sql="sql/ledger.sql")

upload_couriers >> upload_deliveries >> upload_dim_couriers >> upload_fct_order_rates >> upload_ledger