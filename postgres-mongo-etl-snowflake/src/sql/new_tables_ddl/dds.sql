create table dds.dim_couriers(
id serial primary key,
courier_id text,
name varchar
);

create table  dds.fct_courier_tips(
id serial primary key,
courier_id text,
courier_tips_sum numeric(14,2)
);

create table  dds.fct_order_rates(
id serial primary key,
order_id text,
order_ts timestamp,
delivery_id text,
address varchar,
delivery_ts timestamp,
courier_id varchar,
rate float,
sum numeric(14,2)
);