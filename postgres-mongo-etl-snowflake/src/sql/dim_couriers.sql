TRUNCATE TABLE dds.dim_couriers RESTART IDENTITY;

INSERT INTO dds.dim_couriers (courier_id,name)
SELECT 
id AS courier_id
,name

FROM  stg.deliverysystem_couriers;