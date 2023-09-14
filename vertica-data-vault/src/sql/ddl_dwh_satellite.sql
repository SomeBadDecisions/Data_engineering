DROP TABLE IF EXISTS RUBTSOV_KA_GMAIL_COM__DWH.s_auth_history;

CREATE TABLE RUBTSOV_KA_GMAIL_COM__DWH.s_auth_history (
hk_l_user_group_activity INT NOT NULL CONSTRAINT fk_activity_auth REFERENCES RUBTSOV_KA_GMAIL_COM__DWH.l_user_group_activity (hk_l_user_group_activity),
user_id_from INT,
event VARCHAR(40),
event_dt DATETIME,
load_dt DATETIME,
load_src VARCHAR(20)
)
order by event 
SEGMENTED BY hk_l_user_group_activity all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);