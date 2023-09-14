# Изменение модели данных интернет-магазина

В рамках расширения интернет-магазина необходимо сделать миграцию данных в отдельные логические таблицы, а затем собрать на них витрину данных.
Это поможет оптимизировать нагрузку на хранилище и позволит аналитикам отвечать на точечные вопросы о тарифах вендоров.
На текущий момент основная информация находится в таблице *shipping*. Так как в ней находится, по сути, весь лог доставки от момента оформления до выдачи заказа, данные несистематизированны и, более того, могут дублироваться.

## 1.1 As is:
<img width="1464" alt="AS_IS" src="https://user-images.githubusercontent.com/63814959/174645196-b37a7492-a730-41e8-8e00-51afb5d657cd.png">

**Название основной витрины:** shipping

**Структура витрины:**
	
- shippingid - уникальный идентификатор доставки.
- saleid - уникальный идентификатор заказа. К одному заказу может быть привязано несколько строчек shippingid, то есть логов, с информацией о доставке.
- vendorid - уникальный идентификатор вендора. К одному вендору может быть привязано множество saleid и множество строк доставки.
- payment - сумма платежа (дублируется).
- shipping_plan_datetime - плановая дата доставки.
- status - статус доставки в таблице shipping по данному shippingid. Может принимать значения in_progress — доставка в процессе, либо finished — доставка завершена.
- state - промежуточные точки заказа, которые изменяются в соответствии с обновлением информации о доставке по времени state_datetime.

	- booked - заказано;
	- fulfillment — заказ доставлен на склад отправки;
	- queued — заказ в очереди на запуск доставки;
	- transition — запущена доставка заказа;
	- pending — заказ доставлен в пункт выдачи и ожидает получения;
	- received — покупатель забрал заказ;
	- returned — покупатель возвратил заказ после того, как его забрал.
	
- state_datetime - время обновления состояния заказа.
- shipping_transfer_description -  строка со значениями transfer_type и transfer_model, записанными через ':'. Пример записи — 1p:car.

	- transfer_type - тип доставки. 1p означает, что компания берёт ответственность за доставку на себя, 3p — что за отправку ответственен вендор;
	- transfer_model - тип доставки. 1p означает, что компания берёт ответственность за доставку на себя, 3p — что за отправку ответственен вендор.
	
- shipping_transfer_rate - процент стоимости доставки для вендора в зависимости от типа и модели доставки, который взимается интернет-магазином для покрытия расходов.
- shipping_country - страна доставки, учитывая описание тарифа для каждой страны.
- shipping_country_base_rate - налог на доставку в страну, который является процентом от стоимости payment_amount.
- vendor_agreement_description - строка, в которой содержатся данные agreementid, agreement_number, agreement_rate, agreement_commission, записанные через разделитель ':'. Пример записи — 12:vsp-34:0.02:0.023.

	- agreementid - идентификатор договора;
	- agreement_number - номер договора в бухгалтерии;
	- agreement_rate - ставка налога за стоимость доставки товара для вендора;
	- agreement_commission - комиссия, то есть доля в платеже являющаяся доходом компании от сделки.
		
**Пример записей:**
shippingid |	saleid|	orderid	|clientid|	payment|	state_datetime|	productid|	description|	vendorid|	namecategory|	base_country|	status|	state|	shipping_plan_datetime|	hours_to_plan_shipping|	shipping_transfer_description|	shipping_transfer_rate|	shipping_country|shipping_country_base_rate|	vendor_agreement_description
-----------|----------|---------|--------|---------|------------------|----------|-------------|------------|---------------|---------------|---------|------|------------------------|-----------------------|------------------------------|------------------------|-----------------|--------------------------|--------------------------------
1|	1|	4973|	46738|	6.06|	42:34.2|	148|	food&healh vendor_1 from norway|	1|	food&healh|	norway|	in_progress|	booked|	43:42.4|	250.02|	1p:train|	0.025|	russia|	0.03|	0:vspn-4092:0.14:0.02
2|	2|	20857|	192314|	21.93|	27:48.3|	108|	food&healh vendor_1 from germany|	1|	food&healh|	germany|	in_progress|	booked|	49:50.5|	132.37|	1p:train|	0.025|	usa|	0.02|	1:vspn-366:0.13:0.01
3|	3|	14315|	132542|	3.1|	33:16.7|	150|	food&healh vendor_1 from usa|	1|	food&healh|	usa|	in_progress|	booked|	33:16.7|	24|	1p:airplane|	0.04|	norway|	0.04|	2:vspn-4148:0.01:0.01
4|	4|	6682|	61662|	8.57|	21:32.4|	174|	food&healh vendor_3 from russia|	3|	food&healh|	russia|	in_progress|	booked|	14:30.1|	185.87|	1p:train|	0.025|	germany|	0.01|	3:vspn-3023:0.05:0.01
5|	5|	25922|	238974|	1.5|	47:46.1|	135|	food&healh vendor_3 from russia|	3|	food&healh|	russia|	in_progress|	booked|	21:08.8|	102.55|	1p:train|	0.025|	norway|	0.04|	3:vspn-3023:0.05:0.01

**DDL таблицы**

```SQL
DROP TABLE IF EXISTS public.shipping;



--shipping
CREATE TABLE public.shipping(
   ID serial ,
   shippingid                         BIGINT,
   saleid                             BIGINT,
   orderid                            BIGINT,
   clientid                           BIGINT,
   payment_amount                          NUMERIC(14,2),
   state_datetime                    TIMESTAMP,
   productid                          BIGINT,
   description                       text,
   vendorid                           BIGINT,
   namecategory                      text,
   base_country                      text,
   status                            text,
   state                             text,
   shipping_plan_datetime            TIMESTAMP,
   hours_to_plan_shipping           NUMERIC(14,2),
   shipping_transfer_description     text,
   shipping_transfer_rate           NUMERIC(14,3),
   shipping_country                  text,
   shipping_country_base_rate       NUMERIC(14,3),
   vendor_agreement_description      text,
   PRIMARY KEY (ID)
);
CREATE INDEX shippingid ON public.shipping (shippingid);
COMMENT ON COLUMN public.shipping.shippingid is 'id of shipping of sale';
```

## 1.2 To be:
<img width="1475" alt="TO_BE" src="https://user-images.githubusercontent.com/63814959/174645334-0c200e6c-a5bd-4963-a2a5-64c1119e5c93.png">

## 2.1 Создадим справочник стоимости доставки в разные страны:

**Наименование справочника:** shipping_country_rates

**Источники:** shipping_country, shipping_country_base_rate

```SQL
drop table if exists shipping_country_rates;

--shipping_country)rates
create table shipping_country_rates (
shipping_country_id serial,
shipping_country text,
shipping_country_base_rate numeric(14,3),
primary key (shipping_country_id)
);


insert into shipping_country_rates
(shipping_country, shipping_country_base_rate)
select distinct    
shipping_country
,shipping_country_base_rate 
from shipping;
```

## 2.2 Создадим справочник тарифов доставки вендора:

**Наименование справочника:** shipping_agreement

**Источник:** vendor_agreement_description

```SQL
DROP TABLE IF EXISTS shipping_agreement;


-- shipping_agreement
create table shipping_agreement (
agreementid BIGINT,
agreement_number text,
agreement_rate numeric(14,3),
agreement_comission numeric(14,3),
primary key (agreementid)
);

insert into shipping_agreement
(agreementid, agreement_number, agreement_rate, agreement_comission)
select distinct 
cast(descr[1] as integer) as agreementid
,descr[2] as agreement_number
,descr[3]::numeric(14,3) as agreement_rate
,descr[4]::numeric(14,3) as agreement_comission
from (select regexp_split_to_array(vendor_agreement_description, E'\\:+') as descr
	  from shipping ) subq;
```

## 2.3 Создадим справочник типов доставки:

**Наименование справочника:** shipping_transfer

**Источник:** shipping_transfer_description, shipping_transfer_rate 

```SQL
drop table if exists shipping_transfer;

--shipping_transfer
create table shipping_transfer (
transfer_type_id serial,
transfer_type text,
transfer_model text,
shipping_transfer_rate numeric(14,3),
primary key(transfer_type_id)
);

insert into shipping_transfer 
(transfer_type, transfer_model, shipping_transfer_rate)
select distinct 
descr[1] as transfer_type
,descr[2] as transfer_model
,shipping_transfer_rate
from(select 
	 regexp_split_to_array(shipping_transfer_description, E'\\:+') as descr 
	 ,shipping_transfer_rate 
	 from shipping) subq ; 
```

## 2.4 Создадим витрину с уникальными доставками:

**Наименование витрины:** shipping_info

**Источники:** shippingid, shipping_plan_datetime, payment_aount, vendorid

**FK на справочники:** shipping_country_rates, shipping_agreement, shipping_transfer 

```SQL
drop table if exists shipping_info;

--shipping_info
create table shipping_info(
shippingid bigint,
shipping_country_id bigint,
agreementid bigint,
transfer_type_id bigint,
shipping_plan_datetime timestamp,
payment_amount numeric(14,2),
vendorid bigint,
primary key (shippingid),
foreign key (shipping_country_id) references shipping_country_rates(shipping_country_id) on update cascade,
foreign key (agreementid) references shipping_agreement(agreementid) on update cascade,
foreign key (transfer_type_id) references shipping_transfer(transfer_type_id) on update cascade
);

insert into shipping_info
(shippingid, shipping_country_id, agreementid,
 transfer_type_id, shipping_plan_datetime, payment_amount, vendorid)
 select 
 s.shippingid
 ,scr.shipping_country_id 
 ,sa.agreementid 
 ,st.transfer_type_id
 ,s.shipping_plan_datetime
 ,s.payment_amount
 ,s.vendorid  
 from shipping s 
 inner join shipping_country_rates scr
 	on s.shipping_country = scr.shipping_country
 inner join shipping_agreement sa 
 	on cast((regexp_split_to_array(s.vendor_agreement_description, E'\\:+'))[1] as integer) = sa.agreementid
 inner join shipping_transfer st 
 	on s.shipping_transfer_description = st.transfer_type || ':' || st.transfer_model
 group by 1,2,3,4,5,6,7 
```

## 2.5 Создадим витрину статусов доставки:

**Наименование витрины:** shipping_status

**Источники:** shippingid, status, state, state_datetime

**Доп.инфо**:
- Данные в таблице должны отражать максимальный status и state по максимальному времени лога state_datetime в таблице shipping.
- *shipping_start_fact_datetime* — это время state_datetime, когда state заказа перешёл в состояние booked.
- *shipping_end_fact_datetime* — это время state_datetime , когда state заказа перешёл в состояние received.

```SQL
drop table if exists shipping_status;

--shipping_status
create table shipping_status(
shippingid bigint,
status text,
state text,
shipping_start_fact_datetime timestamp,
shipping_end_fact_datetime timestamp,
primary key(shippingid)
);

insert into shipping_status
(shippingid, status, state, shipping_start_fact_datetime,
 shipping_end_fact_datetime)
 
 with max_dttm as (
 select 
 shippingid
 ,max(state_datetime) as max_state_dttm
 ,max(case when state = 'booked' then state_datetime end) as shipping_start_fact_datetime
 ,max(case when state = 'recieved' then state_datetime end) as shipping_end_fact_datetime
 from shipping s 
 group by 1
 )
 
 select 
 t1.shippingid
 ,t2.status
 ,t2.state 
 ,t1.shipping_start_fact_datetime
 ,t1.shipping_end_fact_datetime
 from max_dttm t1
 inner join shipping t2
 	on t1.shippingid = t2.shippingid 
 	and t1.max_state_dttm = t2.state_datetime;
```

## 3.1 Создадим итоговое представление:

**Наименование представления:** shipping_datamart

**Список полей:** 

- shippingid
- vendorid
- transfer_type — тип доставки из таблицы shipping_transfer
- full_day_at_shipping — количество полных дней, в течение которых длилась доставка. Высчитывается как:shipping_end_fact_datetime-shipping_start_fact_datetime.
- is_delay — статус, показывающий просрочена ли доставка. Высчитывается как:shipping_end_fact_datetime >> shipping_plan_datetime → 1 ; 0
- is_shipping_finish — статус, показывающий, что доставка завершена. Если финальный status = finished → 1; 0
- delay_day_at_shipping — количество дней, на которые была просрочена доставка. Высчитыается как: shipping_end_fact_datetime >> shipping_end_plan_datetime → shipping_end_fact_datetime -− shipping_plan_datetime ; 0).
- payment_amount — сумма платежа пользователя
- vat — итоговый налог на доставку. Высчитывается как: payment_amount *∗ ( shipping_country_base_rate ++ agreement_rate ++ shipping_transfer_rate) .
- profit — итоговый доход компании с доставки. Высчитывается как: payment_amount*∗ agreement_commission.

```SQL
create or replace view shipping_datamart as 
select 
si.shippingid
,si.vendorid
,st.transfer_type
,date_part('day',ss.shipping_end_fact_datetime - ss.shipping_start_fact_datetime) as full_day_at_shipping
,case when ss.shipping_end_fact_datetime > si.shipping_plan_datetime then 1 else 0 end as is_delay
,case when ss.status = 'finished' then 1 else 0 end as is_shipping_finish
,case when ss.shipping_end_fact_datetime > si.shipping_plan_datetime 
	  then date_part('day', ss.shipping_end_fact_datetime - si.shipping_plan_datetime) end as delay_day_at_shipping
,si.payment_amount 
,si.payment_amount * (scr.shipping_country_base_rate + sa.agreement_rate + st.shipping_transfer_rate) as vat
,si.payment_amount  * sa.agreement_comission as profit
from shipping_info si
left join shipping_status ss
	on si.shippingid = ss.shippingid 
left join shipping_transfer st
	on si.transfer_type_id = st.transfer_type_id
left join shipping_country_rates scr 
	on si.shipping_country_id = scr.shipping_country_id
left join shipping_agreement sa 
	on si.agreementid = sa.agreementid;
```

## Итог

Существующая модель данных была переботана. С подобной моделью данных интернет-магазин может продолжать расширение не опасаясь высокой нагрузки на базу данных.
Подготовлено представление для аналитиков с готовыми расчетными показателями, которые позволят ускорить их работу. 
