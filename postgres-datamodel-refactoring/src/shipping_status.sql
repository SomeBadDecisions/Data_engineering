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