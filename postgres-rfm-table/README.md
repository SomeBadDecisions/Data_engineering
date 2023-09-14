# Витрина RFM

## 1.1. Выясним требования к целевой витрине.

Название витрины: Витрина для RFM-сегментации клиентов в анализе сбыта по лояльности

Расположение витрины : analysis.dm_rfm_segments

Глубина данных: с начала 2021 года

Периодичность обновления: не обновляется

Структура витрины:
	
- user_id - ID клиента
- recency - метрика, сотрирующая клиентов по дате последнего заказа (1-самые давние, 5-заказывали недавно)
- frequency - метрика, сортирующая клиентов по частоте заказов (1-заказывают реже всех, 5-чазе всех)
- monetary - метрика, сортирующая клиентов по сумме заказов (1-самые маленькие суммы, 5-самые крупные)
	
Дополнительная информация: при сборке витрины должны учитываться только заказы в статусе 'closed'

## 1.2. Изучим структуру исходных данных.

В качестве источников будем использовать следующие таблицы и их поля:

**production.orders:**
	
- order_ts - дата и время заказа
- user_id - ID пользователя
- cost - сумма заказа
- order_id - ID заказа
- status - код статуса заказа 
		
**production.orderstatuses:**
	
- id - ID статуса заказа 
- key - наименование статуса заказа 

**production.users:**

- id - ID клиента


## 1.3. Проанализируем качество данных

В полях, используемых для построения витрины нет NULL-ов и дублей, т.к. при построении витрины были установлены ограничения на содержимоей полей.
Подробно ограничения указаны ниже:
 
 Table.column  | Constraint
------------- | ------------- 
orders.order_ts  | NOT NULL  
orders.user_id  | NOT NULL
orders.cost  | NOT NULL; CHECK ((cost = (payment + bonus_payment)))  
orders.order_id  | NOT NULL; PRIMARY KEY
orders.status | NOT NULL  
orderstatuses.id  | NOT NULL; PRIMARY KEY
orderstatuses.key  | NOT NULL  
users.id  | NOT NULL; PRIMARY KEY

## 1.4.1. Сделаем VIEW для таблиц из базы production (по условию задачи, в коде витрины обращаться можно только к схеме analysis).

```SQL
create or replace view analysis.orders as select * from production.orders;
create or replace view analysis.orderitems as select * from production.orderitems;
create or replace view analysis.orderstatuses as select * from production.orderstatuses;
create or replace view analysis.orderstatuslog as select * from production.orderstatuslog;
create or replace view analysis.products as select * from production.products;
create or replace view analysis.users as select * from production.users;
```

## 1.4.2. Напишем DDL-запрос для создания витрины.

```SQL
CREATE TABLE analysis.dm_rfm_segments (
user_id bigint PRIMARY KEY,
recency smallint NOT NULL,
frequency smallint NOT NULL,
monetary smallint NOT NULL)
```
## 1.4.3 Напишем DDL-запрос для создания промежуточных витрин по каждой метрике

```SQL
CREATE TABLE analysis.tmp_rfm_recency (
 user_id INT NOT NULL PRIMARY KEY,
 recency INT NOT NULL CHECK(recency >= 1 AND recency <= 5)
);
CREATE TABLE analysis.tmp_rfm_frequency (
 user_id INT NOT NULL PRIMARY KEY,
 frequency INT NOT NULL CHECK(frequency >= 1 AND frequency <= 5)
);
CREATE TABLE analysis.tmp_rfm_monetary_value (
 user_id INT NOT NULL PRIMARY KEY,
 monetary_value INT NOT NULL CHECK(monetary_value >= 1 AND monetary_value <= 5)
);
```

## 1.4.4 Напишем SQL-запрос для заполнения промежуточных витрин

**Отдельно отмечу, что по требованию заказчика, клиентов нужно разделить на РАВНЫЕ группы, т.е. если у двух и более клиентов совпадают значения метрик, мы можем ставить им разные оценки, если часть из них "не влезают" в определнную группу (как бы странно это не звучало). Более того, срез нужен единоразово, обновления не предусмотрены. Реализовано через ROW_NUMBER, а не через PERCENTILE_DISC (например) именно по согласованию с заказчиком.** 

**1. tmp_rfm_frequency:**

```SQL 
INSERT INTO analysis.tmp_rfm_frequency (
user_id,
frequency)


with pre as (
select 
t0.id as user_id
, count(t1.order_id) as metric
, max(t2.key) as status
from analysis.users t0
left join analysis.orders t1
	on t0.id = t1.user_id 
left join analysis.orderstatuses t2 
	on t1.status = t2.id 
where (extract('year' from order_ts)) >= 2021								 
group by 1 
),

rn as (
select 
user_id
,metric
,status
,row_number () over (order by status, metric) as rn 
from pre 
)

select 
user_id
,case when rn <= 200 then 1
	  when rn > 200 and rn <= 400 then 2
	  when rn > 400 and rn <= 600 then 3
	  when rn > 600 and rn <= 800 then 4
	  else 5 end as metric
from rn 
```

**2. tmp_rfm_monetary_value:**

```SQL
INSERT INTO analysis.tmp_rfm_monetary_value (
user_id,
monetary_value)


with pre as (
select 
t0.id as user_id
, sum(t1.cost) as metric
, max(t2.key) as status
from analysis.users t0
left join analysis.orders t1
	on t0.id = t1.user_id 
left join analysis.orderstatuses t2 
	on t1.status = t2.id 
where (extract('year' from order_ts)) >= 2021								 
group by 1 
),

rn as (
select 
user_id
,metric
,status
,row_number () over (order by status, metric) as rn 
from pre 
)

select 
user_id
,case when rn <= 200 then 1
	  when rn > 200 and rn <= 400 then 2
	  when rn > 400 and rn <= 600 then 3
	  when rn > 600 and rn <= 800 then 4
	  else 5 end as metric
from rn 
```

**3.tmp_rfm_recency:**

```SQL
INSERT INTO analysis.tmp_rfm_recency (
user_id,
recency)


with pre as (
select 
t0.id as user_id
, max(t1.order_ts) as latest_order
, max(t2.key) as status
from analysis.users t0
left join analysis.orders t1
	on t0.id = t1.user_id 
left join analysis.orderstatuses t2 
	on t1.status = t2.id 
where (extract('year' from order_ts)) >= 2021								 
group by 1 
),

rn as (
select 
user_id
,latest_order
,status
,row_number () over (order by status, latest_order) as rn 
from pre 
)

select 
user_id
,case when rn <= 200 then 1
	  when rn > 200 and rn <= 400 then 2
	  when rn > 400 and rn <= 600 then 3
	  when rn > 600 and rn <= 800 then 4
	  else 5 end as metric
from rn 
```

## 1.4.5 Напишем SQL запрос для создания промежуточной витрины по каждой метрике и её заполнения



```SQL
INSERT INTO analysis.dm_rfm_segments (
user_id,
recency,
frequency,
monetary)

SELECT 
r.user_id 
,r.recency
,f.frequency
,m.monetary_value
FROM analysis.tmp_rfm_recency r
inner join analysis.tmp_rfm_frequency f 
	on r.user_id = f.user_id
inner join analysis.tmp_rfm_monetary_value m
	on r.user_id = m.user_id


```
## 2.1 Изменение структуры витрины

По условиям задачи, во втором задании структура данных в схеме production обновилась: в таблице Orders больше нет поля статуса. А это поле необходимо, потому что для анализа нужно выбрать только успешно выполненные заказы со статусом closed.
Вместо поля с одним статусом разработчики добавили таблицу для журналирования всех изменений статусов заказов — **production.OrderStatusLog.**

Структура таблицы **production.OrderStatusLog:**

- id — синтетический автогенерируемый идентификатор записи,
- order_id — идентификатор заказа, внешний ключ на таблицу production.Orders,
- status_id — идентификатор статуса, внешний ключ на таблицу статусов заказов production.OrderStatuses,
- dttm — дата и время получения заказом этого статуса.

Необходимо внести изменения в то, как формируется представление **analysis.Orders**: вернуть в него поле status. Значение в этом поле должно соответствовать последнему по времени статусу из таблицы **production.OrderStatusLog.**

```SQL
DROP VIEW IF EXISTS analysis.Orders;

ALTER TABLE production.Orders DROP COLUMN status;

CREATE VIEW analysis.Orders AS SELECT * FROM production.Orders;
```

## 2.2 Напишем SQL-запрос для создания и заполнения analysis.orders с учетом всех изменений:

```SQL

create or replace view analysis.orders as 
select * from (
with max_dttm AS( 
select order_id  
,max(dttm) as max_dttm 
from production.orderstatuslog 
group by 1 
), 

order_status as ( 
select t1.order_id 
,t2.status_id  
from max_dttm t1  
inner join production.orderstatuslog t2 
	on t1.order_id = t2.order_id  
	and t1.max_dttm = t2.dttm  
) 

select t1.*
,t2.status_id as status 
from production.orders t1 
left join order_status t2 
	on t1.order_id = t2.order_id 
)subq
```
