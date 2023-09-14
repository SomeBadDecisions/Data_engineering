DROP TABLE IF EXISTS shipping_agreement;


-- shipping_agreement
create table shipping_agreement (
agreementid BIGINT,
agreement_number text,
agreement_rate numeric(14,3),
agreement_comission numeric(14,3),
primary key (agreementid)
);

insert into shipping_agreement
(agreementid, agreement_number, agreement_rate, agreement_comission)
select distinct 
cast(descr[1] as integer) as agreementid
,descr[2] as agreement_number
,descr[3]::numeric(14,3) as agreement_rate
,descr[4]::numeric(14,3) as agreement_comission
from (select regexp_split_to_array(vendor_agreement_description, E'\\:+') as descr
	  from shipping ) subq;