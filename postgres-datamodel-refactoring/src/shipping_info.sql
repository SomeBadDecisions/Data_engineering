drop table if exists shipping_info;

--shipping_info
create table shipping_info(
shippingid bigint,
shipping_country_id bigint,
agreementid bigint,
transfer_type_id bigint,
shipping_plan_datetime timestamp,
payment_amount numeric(14,2),
vendorid bigint,
primary key (shippingid),
foreign key (shipping_country_id) references shipping_country_rates(shipping_country_id) on update cascade,
foreign key (agreementid) references shipping_agreement(agreementid) on update cascade,
foreign key (transfer_type_id) references shipping_transfer(transfer_type_id) on update cascade
);

insert into shipping_info
(shippingid, shipping_country_id, agreementid,
 transfer_type_id, shipping_plan_datetime, payment_amount, vendorid)
 select 
 s.shippingid
 ,scr.shipping_country_id 
 ,sa.agreementid 
 ,st.transfer_type_id
 ,s.shipping_plan_datetime
 ,s.payment_amount
 ,s.vendorid  
 from shipping s 
 inner join shipping_country_rates scr
 	on s.shipping_country = scr.shipping_country
 inner join shipping_agreement sa 
 	on cast((regexp_split_to_array(s.vendor_agreement_description, E'\\:+'))[1] as integer) = sa.agreementid
 inner join shipping_transfer st 
 	on s.shipping_transfer_description = st.transfer_type || ':' || st.transfer_model
 group by 1,2,3,4,5,6,7 