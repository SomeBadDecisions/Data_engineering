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