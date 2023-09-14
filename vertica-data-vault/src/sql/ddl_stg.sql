DROP TABLE IF EXISTS RUBTSOV_KA_GMAIL_COM__STAGING.group_log;

CREATE TABLE RUBTSOV_KA_GMAIL_COM__STAGING.group_log (
group_id INT PRIMARY KEY,
user_id INT,
user_id_from INT,
event varchar(40),
datetime datetime
)
ORDER BY group_id, user_id 
segmented by hash(group_id) all nodes
PARTITION BY datetime::date
GROUP BY calendar_hierarchy_day(datetime::date,3,2);