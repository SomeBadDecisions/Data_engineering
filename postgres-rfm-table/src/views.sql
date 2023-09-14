create or replace view analysis.orders as select * from production.orders;
create or replace view analysis.orderitems as select * from production.orderitems;
create or replace view analysis.orderstatuses as select * from production.orderstatuses;
create or replace view analysis.orderstatuslog as select * from production.orderstatuslog;
create or replace view analysis.products as select * from production.products;
create or replace view analysis.users as select * from production.users;
