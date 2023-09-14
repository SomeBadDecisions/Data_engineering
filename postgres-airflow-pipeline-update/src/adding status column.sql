alter table staging.user_order_log 
add column status varchar(30) DEFAULT 'shipped';

alter table mart.f_sales 
add column status varchar(30) DEFAULT 'shipped';