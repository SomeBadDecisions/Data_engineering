# Построение DWH

## 1.1 Описание
Ранее мною был спроектирован DWH для ресторана, который решил добавить возможность доставки еды. Новые данные необходимо было корректно собирать и обрабатывать. В качестве источников данных использовались **PostgreSql** и **MongoDB**.
Созданный DWH имеет 3 слоя: 

- staging
- dds
- cdm 

Итоговые DAG'и находятся в **/src/dags/existing_dags/examples** 

В рамках данного проекта необходимо доработать существующих DWH, добавив новые источники данных и витрину, содерждащие информацию для расчетов с курьерами.
Заказчику необходимо рассчитать суммы оплаты каждому курьеру за предыдущий месяц. Например, в июне выплачивают сумму за май. Расчётным числом является 10-е число каждого месяца.

Основная задача — реализация ETL-процессов, которые будут преобразовывать и перемещать данные от источников до конечных слоёв данных DWH.

## 1.2 Целевая витрина 
Витрина должна называться **cdm.dm_courier_ledger** и содержать следующие поля:

- id — идентификатор записи.
- courier_id — ID курьера, которому перечисляем.
- courier_name — ФИО курьера.
- settlement_year — год отчёта.
- settlement_month — месяц отчёта, где 1 — январь и 12 — декабрь.
- orders_count — количество заказов за период (месяц).
- orders_total_sum — общая стоимость заказов.
- rate_avg — средний рейтинг курьера по оценкам пользователей.
- order_processing_fee — сумма, удержанная компанией за обработку заказов, которая высчитывается как orders_total_sum * 0.25.
- courier_order_sum — сумма, которую необходимо перечислить курьеру за доставленные им/ей заказы. За каждый доставленный заказ курьер должен получить некоторую сумму в зависимости от рейтинга (см. ниже).
- courier_tips_sum — сумма, которую пользователи оставили курьеру в качестве чаевых.
- courier_reward_sum — сумма, которую необходимо перечислить курьеру. Вычисляется как courier_order_sum + courier_tips_sum * 0.95 (5% — комиссия за обработку платежа).

Правила расчёта процента выплаты курьеру в зависимости от рейтинга, где r — это средний рейтинг курьера в текущем месяце:

r < 4 — 5% от заказа, но не менее 100 руб.;

4 <= r < 4.5 — 7% от заказа, но не менее 150 руб.;

4.5 <= r < 4.9 — 8% от заказа, но не менее 175 руб.;

4.9 <= r — 10% от заказа, но не менее 200 руб.

Данные о заказах уже есть в хранилище. Данные курьерской службы необходимо забрать из API курьерской службы, после чего совместить их с данными подсистемы заказов.

## 1.3 Спецификация API 
**API-KEY**: 25c27781-8fde-4b30-a22e-524044a7580f
Для обращения к API необходима утилита curl.

Важно учесть, что данных много, API отдают их порционно, поэтому для корректного считывания данных нужно будет воспользоваться полями offset, limit, from и to.

### 1.3.1 GET /restaurants
Метод /restaurants возвращает список доступных ресторанов (_id, name).

```console
curl --location --request GET 'https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/restaurants?sort_field={{ sort_field }}&sort_direction={{ sort_direction }}&limit={{ limit }}&offset={{ offset }}' \
--header 'X-Nickname: MrK' \
--header 'X-Cohort: 6' \
--header 'X-API-KEY: {{ api_key }}'
# вставить API-KEY без двойных скобок
```

Описание параметров запроса, указанных в {{ }} :

- sort_field — необязательный параметр. Возможные значения: id, name. Значение по умолчанию: id.
Параметр определяет поле, к которому будет применяться сортировка, переданная в параметре sort_direction.
- sort_direction — необязательный параметр. Возможные значения: asc, desc.
Параметр определяет порядок сортировки для поля, переданного в sort_field:
asc — сортировка по возрастанию,
desc — сортировка по убыванию.
- limit — необязательный параметр. Значение по умолчанию: 50. Возможные значения: целое число в интервале от 0 до 50 включительно.
Параметр определяет максимальное количество записей, которые будут возвращены в ответе.
- offset — необязательный параметр. Значение по умолчанию: 0.
Параметр определяет количество возвращаемых элементов результирующей выборки, когда формируется ответ.

### 1.3.2 GET /couriers
Метод /couriers используется для того, чтобы получить список курьеров (_id, name).

```console
curl --location --request GET 'https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/couriers?sort_field={{ sort_field }}&sort_direction={{ sort_direction }}&limit={{ limit }}&offset={{ offset }}' \
--header 'X-Nickname: MrK' \
--header 'X-Cohort: 6' \
--header 'X-API-KEY: {{ api_key }}' 
# вставить API-KEY без двойных скобок
```

Параметры /couriers аналогичны параметрам в предыдущем методе: sort_field, sort_direction, limit, offset.

### 1.3.3 GET / deliveries
Метод /deliveries используется для того, чтобы получить список совершённых доставок.

```console
curl --location --request GET 'https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/deliveries?restaurant_id={{ restaurant_id }}&from={{ from }}&to={{ to }}&sort_field={{ sort_field }}&sort_direction={{ sort_direction }}&limit={{ limit }}&offset={{ limit }}' \
--header 'X-Nickname: MrK' \
--header 'X-Cohort: 6' \
--header 'X-API-KEY: {{ api_key }}'
# вставить API-KEY без двойных скобок
```

Параметры метода также включают sort_field, sort_direction, limit, offset и несколько дополнительных полей:

- sort_field — необязательный параметр. Возможные значения: _id, name. Если указать _id, то сортировка будет применяться к ID заказа (order_id). Если указать date, то сортировка будет применяться к дате создания заказа (order_ts).
- restaurant_id — ID ресторана. Если значение не указано, то метод вернёт данные по всем доступным в БД ресторанам.
- from и to — параметры фильтрации. В выборку попадают заказы с датой доставки между from и to.

Метод возвращает список совершённых доставок с учётом фильтров в запросе. Каждый элемент списка содержит:

- order_id — ID заказа;
- order_ts — дата и время создания заказа;
- delivery_id — ID доставки;
- courier_id — ID курьера;
- address — адрес доставки;
- delivery_ts — дата и время совершения доставки;
- rate — рейтинг доставки, который выставляет покупатель. Целочисленное значение от 1 до 5;
- sum — сумма заказа (в руб.);
- tip_sum — сумма чаевых, которые оставил покупатель курьеру (в руб.).

## 2 Проектирование хранилища

Учитывая кокнретные потребности бизнеса проектирование начнем с итоговогй витрины в cdm-слое.

### 2.1 CDM 

```SQL
create table cdm.dm_courier_ledger (
id serial primary key,
courier_id varchar,
courier_name varchar,
settlement_year integer,
settlement_month integer,
orders_count integer,
orders_total_sum numeric(14,2),
rate_avg numeric(14,2),
order_processing_fee float,
courier_order_sum float,
courier_tips_sum numeric(14,2),
courier_reward_sum float);
```

### 2.2 STG

STG слой будет хранить данные AS IS.

**stg.deliverysystem_couriers**

```SQL
create table stg.deliverysystem_couriers (
id text not NULL,
name varchar
);
```

**stg.deliverysystem_deliveries**

```SQL
create table stg.deliverysystem_deliveries (
order_id text not null,
order_ts timestamp,
delivery_id text not null,
courier_id text not null,
address varchar,
delivery_ts timestamp,
rate numeric(14,2),
sum float,
tip_sum numeric(14,2)
);
```

### 2.3 DDS

В качестве модели данных выбрана "снежинка".

**dds.dim_couriers**

```SQL 
create table dds.dim_couriers(
id serial primary key,
courier_id text,
name varchar
);
```

**dds.fct_order_rates**

```SQL
create table  dds.fct_order_rates(
id serial primary key,
order_id text,
order_ts timestamp,
delivery_id text,
courier_id varchar,
address varchar,
delivery_ts timestamp,
rate float,
sum numeric(14,2),
tip_sum numeric(14,2)
);
```

## 3 Реализация DAG

Далее необходимо реализовать новые DAG'и для корректного извлечения и обработки данных.

### 3.1 STG

Начнем с заливки данных в слой стейджинга.

Здесь у нас 2 таблицы **deliverysystem_couriers** и **deliverysystem_deliveries**, забираем данные по API и заливаем в таблицы as is.
Дополнительно позаботимся об идемпотентности (добавим очистку таблиц перед заливкой, чтобы при последующих запусках дага данные не дублировались).

Итоговый даг находится в **/src/dags/delivery_system.py** и выглядит следующим образом:

```python
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


upload_couriers >> upload_deliveries
```

### 3.2 DDS 

Далее прольем данные в DDS. Заполнить нужно 2 таблицы: **dim_couriers** и **fct_order_rates**.

Напишем SQL-скрипт и добавим в написанный ранее DAG PostgresOperator для каждой целевой таблицы.

```SQL
TRUNCATE TABLE dds.dim_couriers RESTART IDENTITY;

INSERT INTO dds.dim_couriers (courier_id,name)
SELECT 
id AS courier_id
,name

FROM  stg.deliverysystem_couriers
;
```

```SQL
TRUNCATE TABLE dds.fct_order_rates RESTART IDENTITY;

INSERT INTO dds.fct_order_rates (order_id,order_ts,delivery_id,address,delivery_ts,courier_id,rate,sum, tip_sum)
SELECT 
order_id 
,order_ts
,delivery_id
,address
,delivery_ts
,rate
,sum
,tip_sum

FROM stg.deliverysystem_deliveries
;
```

```python
upload_dim_couriers = PostgresOperator(
        task_id='dim_couriers',
        postgres_conn_id=postgres_conn,
        sql="sql/dim_couriers.sql")
```

```python
upload_fct_order_rates = PostgresOperator(
        task_id='fct_order_rates',
        postgres_conn_id=postgres_conn,
        sql="sql/fct_order_rates.sql")
```

### 3.3 CDM 

Теперь заполним итоговую витрину **cdm.dm_courier_ledger**.

```SQL
TRUNCATE TABLE cdm.dm_courier_ledger RESTART IDENTITY;

INSERT INTO cdm.dm_courier_ledger (courier_id, courier_name, settlement_year, settlement_month, orders_count, orders_total_sum, rate_avg, 
								   order_processing_fee, courier_order_sum, courier_tips_sum, courier_reward_sum)
with pre as (
SELECT 
ort.courier_id
,c.name as courier_name 
,EXTRACT('year' from ort.delivery_ts) as settlement_year
,EXTRACT('month' from ort.delivery_ts) as settlement_month 
,COUNT(ort.order_id) as orders_count
,SUM(ort.sum) as orders_total_sum
,AVG(ort.rate) as rate_avg
,SUM(ort.sum) * 0.25 as order_processing_fee   
,SUM(ort.tip_sum) as courier_tips_sum

FROM dds.fct_order_rates ort
LEFT JOIN dds.dim_couriers c 
	ON ort.courier_id = c.courier_id
group by 1,2,3,4
),


pre_reward as (
select
ort.courier_id
,ort.sum 
,pre.rate_avg 
,EXTRACT('year' from ort.delivery_ts) as settlement_year
,EXTRACT('month' from ort.delivery_ts) as settlement_month 
,case when pre.rate_avg < 4 then 
			case when ort.sum * 0.05 >= 100 then ort.sum * 0.05 else 100 END
	  when pre.rate_avg >= 4 and pre.rate_avg < 4.5 then 
	  		case when ort.sum * 0.07 >= 150 then ort.sum * 0.07 else 150 end  
	  when pre.rate_avg >= 4.5 and pre.rate_avg < 4.9 then 
	  		case when ort.sum * 0.08 >= 175 then ort.sum * 0.08 else 175 END
	  when pre.rate_avg >= 4.9 then 
	  		case when ort.sum * 0.08 >= 200 then ort.sum * 0.08 else 200 end  
	  end as courier_order 
from dds.fct_order_rates ort
left join pre 
	on ort.courier_id = pre.courier_id
	and EXTRACT('year' from ort.delivery_ts) = pre.settlement_year
	and EXTRACT('month' from ort.delivery_ts) = pre.settlement_month
),

reward as (
select 
courier_id
,settlement_year
,settlement_month
,SUM(courier_order) as courier_order_sum

from pre_reward
group by 1,2,3
)

select 
t1.courier_id
,t1.courier_name
,t1.settlement_year
,t1.settlement_month
,t1.orders_count
,t1.orders_total_sum
,t1.rate_avg
,t1.order_processing_fee
,t2.courier_order_sum
,t1.courier_tips_sum
,t2.courier_order_sum + courier_tips_sum * 0.95 as courier_reward_sum

from pre t1
left join reward t2 
	on t1.courier_id = t2.courier_id
	and t1.settlement_year = t2.settlement_year
	and t1.settlement_month = t2.settlement_month
order by courier_id;
```

И добавим еще один PostgresOperator:

```python 
upload_ledger = PostgresOperator(
        task_id='ledger',
        postgres_conn_id=postgres_conn,
        sql="sql/ledger.sql")
```

## Итоги 

В рамках проекта был организован ETL-процесс, забирающий данные по API и через stg и dds заливающий их в витрину для рассчетов с курьерами **cdm.dm_courier_ledger**.

Все итоговые sql скрипты находятся в папке **/src/sql**

Итоговый даг находится в **/src/dags/delivery_system.py**
 


