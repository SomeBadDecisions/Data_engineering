DROP TABLE IF EXISTS RUBTSOV_KA_GMAIL_COM__DWH.l_user_group_activity;

CREATE TABLE RUBTSOV_KA_GMAIL_COM__DWH.l_user_group_activity (
hk_l_user_group_activity INT PRIMARY KEY,
hk_user_id INT NOT NULL CONSTRAINT fk_l_group_user REFERENCES RUBTSOV_KA_GMAIL_COM__DWH.h_users (hk_user_id),
hk_group_id INT NOT NULL CONSTRAINT fk_l_user_group REFERENCES RUBTSOV_KA_GMAIL_COM__DWH.h_groups (hk_group_id),
load_dt DATETIME,
load_src VARCHAR(20)
)
order by load_dt
SEGMENTED BY hk_l_user_group_activity all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);