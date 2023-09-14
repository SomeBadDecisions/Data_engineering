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
