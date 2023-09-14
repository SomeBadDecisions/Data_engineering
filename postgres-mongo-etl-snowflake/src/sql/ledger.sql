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