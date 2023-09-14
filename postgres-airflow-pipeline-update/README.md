# Изменение существующего пайплайна

## 1.1 Описание
В рамках данного проекта необходимо изменить процессы в пайплайне так, чтобы они соответствовали новым задачам бизнеса.
 
Существует витрина **mart.f_sales** со следующими полями:

- id - идентификатор продажи (serial)
- date_id - дата продаж в формате YYYYMMDD (int)
- item_id - идентификатор продукта  (int)
- customer_id - идентификатор клиента  (int)
- сity_id - идентификатор город клиента (int)
- quantity - количество купленного товара (double(10,2))
- amount - сумма покупки (double(10,2))

В ходе развития бизнеса, команда разработки добавила функционал отмены заказов и возврата средств (refunded). Значит, процессы в пайплайне нужно обновить.

Новые инкременты с информацией о продажах приходят по API и содержат статус заказа (shipped/refunded).

## 1.2 Спецификация API 
**API-KEY**: 5f55e6c0-e9e5-4a9c-b313-63c01fc31460
Для обращения к API необходима утилита curl.

## 1.2.1 POST /generate_report
Метод /generate_report инициализирует формирование отчёта. 
Метод возвращает task_id — ID задачи, в результате выполнения которой должен сформироваться отчёт.

Обращение к API с помощью curl:

```console
curl --location --request POST 'https://d5dg1j9kt695d30blp03.apigw.yandexcloud.net/generate_report' \
--header 'X-Nickname: MrK' \
--header 'X-Cohort: 3' \
--header 'X-Project: True' \
--header 'X-API-KEY: {{ api_key }}' 
# вставить API-KEY без двойных скобок
```

## 1.2.2 GET /get_report
Метод get_report используется для получения отчёта после того, как он будет сформирован на сервере.
Пока отчёт будет формироваться, будет возвращаться статус RUNNING.
Если отчёт сформирован, то метод вернёт статус SUCCESS и report_id.

Обращение к API с помощью curl:

```console
curl --location --request GET 'https://d5dg1j9kt695d30blp03.apigw.yandexcloud.net/get_report?task_id={{ task_id }}' \
--header 'X-Nickname: MrK' \
--header 'X-Cohort: 3' \
--header 'X-Project: True' \
--header 'X-API-KEY: {{ api_key }}'
# вставить API-KEY без двойных скобок 
```

Сформированный отчёт содержит четыре файла:

- custom_research.csv,
- user_orders_log.csv,
- user_activity_log.csv,
- price_log.csv.
Файлы отчетов можно получить по URL из параметра s3_path.

## 1.2.3 GET /get_increment
Метод get_increment используется для получения данных за те даты, которые не вошли в основной отчёт. Дата обязательно в формате 2020-01-22T00:00:00.
Если инкремент сформирован, то метод вернёт статус SUCCESS и increment_id. Если инкремент не сформируется, то вернётся NOT FOUND с описанием причины.

Обращение к API с помощью curl:

```console
curl --location --request GET 'https://d5dg1j9kt695d30blp03.apigw.yandexcloud.net/get_increment?report_id={{ report_id }}&date={{ date }}' \
--header 'X-Nickname: MrK' \
--header 'X-Cohort: 3' \
--header 'X-Project: True' \
--header 'X-API-KEY: {{ api_key }}'
# вставить API-KEY без двойных скобок 
```

Сформированный инкремент содержит четыре файла: 

- custom_research_inc.csv 
- user_orders_log_inc.csv 
- user_activity_log_inc.csv
- price_log_inc.csv
Файлы отчетов можно получить по URL из параметра s3_path.

## 1.2.4 Текущий пайплайн в AirFlow

**DAG:**

```python
import time
import requests
import json
import pandas as pd

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.base import BaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.http_hook import HttpHook

http_conn_id = HttpHook.get_connection('http_conn_id')
api_key = http_conn_id.extra_dejson.get('api_key')
base_url = http_conn_id.host

postgres_conn_id = 'postgresql_de'

nickname = 'MrK'
cohort = '3'

headers = {
    'X-Nickname': nickname,
    'X-Cohort': cohort,
    'X-Project': 'True',
    'X-API-KEY': api_key,
    'Content-Type': 'application/x-www-form-urlencoded'
}


def generate_report(ti):
    print('Making request generate_report')

    response = requests.post(f'{base_url}/generate_report', headers=headers)
    response.raise_for_status()
    task_id = json.loads(response.content)['task_id']
    ti.xcom_push(key='task_id', value=task_id)
    print(f'Response is {response.content}')


def get_report(ti):
    print('Making request get_report')
    task_id = ti.xcom_pull(key='task_id')

    report_id = None

    for i in range(20):
        response = requests.get(f'{base_url}/get_report?task_id={task_id}', headers=headers)
        response.raise_for_status()
        print(f'Response is {response.content}')
        status = json.loads(response.content)['status']
        if status == 'SUCCESS':
            report_id = json.loads(response.content)['data']['report_id']
            break
        else:
            time.sleep(10)

    if not report_id:
        raise TimeoutError()

    ti.xcom_push(key='report_id', value=report_id)
    print(f'Report_id={report_id}')


def get_increment(date, ti):
    print('Making request get_increment')
    report_id = ti.xcom_pull(key='report_id')
    response = requests.get(
        f'{base_url}/get_increment?report_id={report_id}&date={str(date)}T00:00:00',
        headers=headers)
    response.raise_for_status()
    print(f'Response is {response.content}')

    increment_id = json.loads(response.content)['data']['increment_id']
    ti.xcom_push(key='increment_id', value=increment_id)
    print(f'increment_id={increment_id}')


def upload_data_to_staging(filename, date, pg_table, pg_schema, ti):
    increment_id = ti.xcom_pull(key='increment_id')
    s3_filename = f'https://storage.yandexcloud.net/s3-sprint3/cohort_{cohort}/{nickname}/project/{increment_id}/{filename}'

    local_filename = date.replace('-', '') + '_' + filename

    response = requests.get(s3_filename)
    open(f"{local_filename}", "wb").write(response.content)

    df = pd.read_csv(local_filename)
    df.drop_duplicates(subset=['id'])

    postgres_hook = PostgresHook(postgres_conn_id)
    engine = postgres_hook.get_sqlalchemy_engine()
    df.to_sql(pg_table, engine, schema=pg_schema, if_exists='append', index=False)


args = {
    "owner": "student",
    'email': ['student@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}

business_dt = '{{ ds }}'

with DAG(
        'customer_retention',
        default_args=args,
        description='Provide default dag for sprint3',
        catchup=True,
        start_date=datetime.today() - timedelta(days=8),
        end_date=datetime.today() - timedelta(days=1),
) as dag:
    generate_report = PythonOperator(
        task_id='generate_report',
        python_callable=generate_report)

    get_report = PythonOperator(
        task_id='get_report',
        python_callable=get_report)

    get_increment = PythonOperator(
        task_id='get_increment',
        python_callable=get_increment,
        op_kwargs={'date': business_dt})

    upload_user_order_inc = PythonOperator(
        task_id='upload_user_order_inc',
        python_callable=upload_data_to_staging,
        op_kwargs={'date': business_dt,
                   'filename': 'user_orders_log_inc.csv',
                   'pg_table': 'user_order_log',
                   'pg_schema': 'staging'})

    update_d_item_table = PostgresOperator(
        task_id='update_d_item',
        postgres_conn_id=postgres_conn_id,
        sql="sql/mart.d_item.sql")

    update_d_customer_table = PostgresOperator(
        task_id='update_d_customer',
        postgres_conn_id=postgres_conn_id,
        sql="sql/mart.d_customer.sql")

    update_d_city_table = PostgresOperator(
        task_id='update_d_city',
        postgres_conn_id=postgres_conn_id,
        sql="sql/mart.d_city.sql")

    update_f_sales = PostgresOperator(
        task_id='update_f_sales',
        postgres_conn_id=postgres_conn_id,
        sql="sql/mart.f_sales.sql",
        parameters={"date": {business_dt}}
    )

    (
            generate_report
            >> get_report
            >> get_increment
            >> upload_user_order_inc
            >> [update_d_item_table, update_d_city_table, update_d_customer_table]
            >> update_f_sales
    )

```

## 1.2.5 Текущие SQL-скрипты
**mart.d_city:**
```sql
insert into mart.d_city (city_id, city_name)
select city_id, city_name from staging.user_order_log
where city_id not in (select city_id from mart.d_city)
group by city_id, city_name;
```

**mart.d_customer:**
```sql
insert into mart.d_customer (customer_id, first_name, last_name, city_id)
select customer_id, first_name, last_name, max(city_id) from staging.user_order_log
where customer_id not in (select customer_id from mart.d_customer)
group by customer_id, first_name, last_name
```

**mart.d_item:**
```sql
insert into mart.d_item (item_id, item_name)
select item_id, item_name from staging.user_order_log
where item_id not in (select item_id from mart.d_item)
group by item_id, item_name
```

**mart.f_sales:**
```sql
insert into mart.f_sales (date_id, item_id, customer_id, city_id, quantity, payment_amount)
select dc.date_id, item_id, customer_id, city_id, quantity, payment_amount from staging.user_order_log uol
left join mart.d_calendar as dc on uol.date_time::Date = dc.date_actual
where uol.date_time::Date = '{{ds}}';
```

## 2.1 Добавление статуса заказа
В рамках доработки необходимо: 

- учесть в витрине mart.f_sales статусы shipped и refunded. Все данные в витрине следует считать shipped
- обновить пайплайн с учётом статусов и backward compatibility

Для выполнения условий добавим столбец **status** в таблицы **staging.user_order_log** и **mart.f_sales**:

```sql
alter table staging.user_order_log 
add column status varchar(30) DEFAULT 'shipped';

alter table mart.f_sales 
add column status varchar(30) DEFAULT 'shipped';
```

Далее обновил функцию upload_data_to_staging. Избавимся от колонки с id, чтобы инкременты за разные даты не конфликтовали:

```python
def upload_data_to_staging(filename, date, pg_table, pg_schema, ti):
    increment_id = ti.xcom_pull(key='increment_id')
    s3_filename = f'https://storage.yandexcloud.net/s3-sprint3/cohort_{cohort}/{nickname}/project/{increment_id}/{filename}'

    local_filename = date.replace('-', '') + '_' + filename

    response = requests.get(s3_filename)
    open(f"{local_filename}", "wb").write(response.content)

    df = pd.read_csv(local_filename)
    df = df.drop_duplicates(subset=['id'])
    df = df.drop(['id'], axis = 1) #Новая строка

    postgres_hook = PostgresHook(postgres_conn_id)
    engine = postgres_hook.get_sqlalchemy_engine()
    df.to_sql(pg_table, engine, schema=pg_schema, if_exists='append', index=False)
```

Следующим шагом исправим запрос на заполнение **mart.f_sales** с учетом добавления статуса.
Дополнительно необходимо учесть, что payment_amount должен быть отрицательным для заказов со статусом refunded.

```sql
insert into mart.f_sales (date_id, item_id, customer_id, city_id, quantity, payment_amount, status)
select 
dc.date_id
,item_id
,customer_id
,city_id
,quantity
,CASE WHEN status = 'refunded' then payment_amount * -1 ELSE payment_amount END as payment_amount
,status 
from staging.user_order_log uol
left join mart.d_calendar as dc on uol.date_time::Date = dc.date_actual
where uol.date_time::Date = '{{ds}}';
```

## 2.2 Новая витрина customer_retention 
Поступили новые требования, в рамках которых нужно разработать витрину **mart.f_customer_retention** со следующей информацией:

- Рассматриваемый период - **weekly**;
- Статусы клиентов:

    - **new** - кол-во клиентов, которые оформили один заказ за рассматриваемый период;
	- **returning** - кол-во клиентов, которые оформили более одного заказа за рассматриваемый период;
	- **refunded** - кол-во клиентов, которые вернули заказ за рассматриваемый период.
	
- **Revenue** и **refunded** для каждой категории покупателей.

**Цель создания витрины:** выяснить, какие категории товаров лучше всего удерживают клиентов.

Схема **mart.f_customer_retention:**

1. **new_customers_count** — кол-во новых клиентов (тех, которые сделали только один 
заказ за рассматриваемый промежуток времени).
2. **returning_customers_count** — кол-во вернувшихся клиентов (тех,
которые сделали только несколько заказов за рассматриваемый промежуток времени).
3. **refunded_customer_count** — кол-во клиентов, оформивших возврат за 
рассматриваемый промежуток времени.
4. **period_name** — weekly.
5. **period_id** — идентификатор периода (номер недели или номер месяца).
6. **item_id** — идентификатор категории товара.
7. **new_customers_revenue** — доход с новых клиентов.
8. **returning_customers_revenue** — доход с вернувшихся клиентов.
9. **customers_refunded** — количество возвратов клиентов. 

Для начала напишем DDL для новой витрины:

```sql
create table if not exists mart.f_customer_retention (
new_customers_count INT,
returning_customers_count INT,
refunded_customer_count INT,
period_name VARCHAR(10),
period_id VARCHAR(20),
item_id INT,
new_customers_revenue INT,
returning_customers_revenue INT,
customers_refunded INT,
foreign key (item_id) references mart.d_item (item_id) on update cascade 
);
```

Далее напишем скрипт для заполнения витрины:

```sql 
truncate table mart.f_customer_retention;

insert into mart.f_customer_retention
(new_customers_count, returning_customers_count, refunded_customer_count, period_name, period_id, item_id, new_customers_revenue,returning_customers_revenue,customers_refunded)

with orders_counter as (
select 
fs2.customer_id
,dc.week_of_year_iso as period_id
,count(fs2.id) as orders_cnt
,MAX(case when status = 'refunded' then customer_id END) as ref_cnt

from mart.f_sales fs2
left join mart.d_calendar dc 
	on fs2.date_id = dc.date_id 
group by 1,2
)


SELECT
count(distinct(case when orders_cnt = 1 then oc.customer_id END)) as  new_customers_count
,count(distinct(case when orders_cnt > 1 then oc.customer_id END)) as returning_customers_count
,count(distinct(ref_cnt)) as refunded_customer_count
,'weekly' as period_name
,oc.period_id
,fs2.item_id 
,sum(case when orders_cnt = 1 then fs2.payment_amount END) as new_customers_revenue
,SUM(case when orders_cnt > 1 then fs2.payment_amount end) as returning_customers_revenue
,count(case when fs2.status = 'refunded' then fs2.id END) as customers_refunded

from mart.f_sales fs2 
left join mart.d_calendar dc 
	on fs2.date_id = dc.date_id 
left join orders_counter oc 
	on fs2.customer_id = oc.customer_id 
	and dc.week_of_year_iso = oc.period_id
group by 5,6;
```

В конце добавим новый **PostgresOperator** в существующий DAG:

```python
update_f_retention_table = PostgresOperator(
        task_id='update_f_retention',
        postgres_conn_id=postgres_conn_id,
        sql="sql/mart.f_customer_retention.sql")
```

Дополнительно заменим стандартные print'ы на логгеры по всему коду для большей гибкости и информативности. Так же использование логгера поможет нам давать более информативные сообщения при возникновении ошибок.

**Полный код получившегося DAG-а лежит в папке DAG.**

## Итог 
В ходе данного проекта был переработан существующий пайплайн данных с учетом новый потребностей бизнеса, а именно:

- добавлено поле, учитывающее статус заказа
- добавлена новая витрина, позволяющая анализировать "возвращаемость клиентов" в разрезе недель и отвечать на вопрос "какой товар лучше удерживает клиентов"