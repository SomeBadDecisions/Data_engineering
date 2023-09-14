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
