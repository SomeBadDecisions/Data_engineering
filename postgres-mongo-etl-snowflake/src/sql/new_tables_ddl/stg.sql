create table stg.deliverysystem_couriers (
id text not NULL,
name varchar
);

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