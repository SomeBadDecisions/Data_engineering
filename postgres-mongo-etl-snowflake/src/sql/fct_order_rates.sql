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

FROM stg.deliverysystem_deliveries;