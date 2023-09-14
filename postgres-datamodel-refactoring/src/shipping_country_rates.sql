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