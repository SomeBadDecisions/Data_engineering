import contextlib
import hashlib
import json
from typing import Dict, List, Optional
 
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
 
from airflow.decorators import dag
 
import pandas as pd
import pendulum
import vertica_python
from airflow.operators.bash import BashOperator
import boto3


default_args = {
    'owner': 'Airflow',
    'retries': 0
}

AWS_ACCESS_KEY_ID = "YCAJEWXOyY8Bmyk2eJL-hlt2K"
AWS_SECRET_ACCESS_KEY = "YCPs52ajb2jNXxOUsL4-pFDL1HnV2BCPd928_ZoA"

session = boto3.session.Session()
s3_client = session.client(
    service_name='s3',
    endpoint_url='https://storage.yandexcloud.net',
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
)



def fetch_s3_file(bucket: str, key: str):
    s3_client.download_file(
        bucket,
        key,
        f'/data/{key}'
    )

def load_dataset_to_vertica(
    dataset_path: str,
    schema: str,
    table: str,
    columns: List[str]
):
    conn_info = {'host': '51.250.75.20',
             'port': 5433,
             'user': 'rubtsov_ka_gmail_com',
             'password': 'etsi1htIOczd'
             }

    with vertica_python.connect(**conn_info) as conn:
        cur = conn.cursor()
        cur.execute(f"TRUNCATE TABLE {schema}.{table}")
        cur.execute(f"""COPY {schema}.{table} ({columns})
                   FROM LOCAL '{dataset_path}'
                   DELIMITER ','
                   """)
                   
        conn.commit()
    conn.close()

def stg_to_link(
    schema: str,
    table: str,
    columns: List[str]
):
    conn_info = {'host': '51.250.75.20',
             'port': 5433,
             'user': 'rubtsov_ka_gmail_com',
             'password': 'etsi1htIOczd'
             }

    with vertica_python.connect(**conn_info) as conn:
        cur = conn.cursor()
        cur.execute(f"TRUNCATE TABLE {schema}.{table}")
        cur.execute(f"""
        INSERT INTO {schema}.{table}({columns})

        select distinct
        hash(hu.user_id,hg.group_id) as hk_l_user_group_activity
        ,hk_user_id
        ,hk_group_id
        ,now() as load_dt
        ,'S3' as load_src

        from RUBTSOV_KA_GMAIL_COM__STAGING.group_log as gl
        left join RUBTSOV_KA_GMAIL_COM__DWH.h_users hu
            on gl.user_id = hu.user_id 
        left join RUBTSOV_KA_GMAIL_COM__DWH.h_groups hg 
            on gl.group_id = hg.group_id 
        ;
                   """)
                   
        conn.commit()
    conn.close()

def stg_to_st(
    schema: str,
    table: str,
    columns: List[str]
):
    conn_info = {'host': '51.250.75.20',
             'port': 5433,
             'user': 'rubtsov_ka_gmail_com',
             'password': 'etsi1htIOczd'
             }

    with vertica_python.connect(**conn_info) as conn:
        cur = conn.cursor()
        cur.execute(f"TRUNCATE TABLE {schema}.{table}")
        cur.execute(f"""
        INSERT INTO {schema}.{table}({columns})

        select 
        luga.hk_l_user_group_activity,
        gl.user_id_from ,
        gl.event,
        gl.datetime as event_dt,
        now() as load_dt,
        'S3' as load_src

        from RUBTSOV_KA_GMAIL_COM__STAGING.group_log as gl
        left join RUBTSOV_KA_GMAIL_COM__DWH.h_groups as hg 
            on gl.group_id = hg.group_id
        left join RUBTSOV_KA_GMAIL_COM__DWH.h_users as hu 
            on gl.user_id = hu.user_id
        left join RUBTSOV_KA_GMAIL_COM__DWH.l_user_group_activity as luga 
            on hg.hk_group_id = luga.hk_group_id 
            and hu.hk_user_id = luga.hk_user_id
            ;
                   """)
                   
        conn.commit()
    conn.close()
 

with DAG(
    dag_id="group_log",
    default_args=default_args,
    start_date=pendulum.datetime(2022, 12, 25, tz="UTC"),
    schedule_interval=None,
    catchup=False
) as dag:
    
    download = PythonOperator(
        task_id='fetch_csv',
        python_callable=fetch_s3_file,
        op_kwargs={'bucket': 'sprint6', 'key': 'group_log.csv'}
    )

    to_stg = PythonOperator(
        task_id='csv_to_stg',
        python_callable=load_dataset_to_vertica,
        op_kwargs={'dataset_path': '/data/group_log.csv',
        'schema': 'RUBTSOV_KA_GMAIL_COM__STAGING',
        'table':'group_log',
        'columns':'group_id,user_id,user_id_from,event,datetime'}
    )

    stg_to_l = PythonOperator(
        task_id='stg_to_l',
        python_callable=stg_to_link,
        op_kwargs={'schema':'RUBTSOV_KA_GMAIL_COM__DWH',
                   'table':'l_user_group_activity',
                   'columns':'hk_l_user_group_activity,hk_user_id,hk_group_id,load_dt,load_src'}
    )

    stg_to_s = PythonOperator(
        task_id='stg_to_s',
        python_callable=stg_to_st,
        op_kwargs={'schema':'RUBTSOV_KA_GMAIL_COM__DWH',
                   'table':'s_auth_history',
                   'columns':'hk_l_user_group_activity,user_id_from,event,event_dt,load_dt,load_src'}
    )


    download >> to_stg >> stg_to_l >> stg_to_s
