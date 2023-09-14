create table if not exists mart.f_customer_retention (
new_customers_count INT,
returning_customers_count INT,
refunded_customer_count INT,
period_name VARCHAR(10),
period_id VARCHAR(20),
item_id INT,
new_customers_revenue INT,
returning_customers_revenue INT,
customers_refunded INT,
foreign key (item_id) references mart.d_item (item_id) on update cascade 
);