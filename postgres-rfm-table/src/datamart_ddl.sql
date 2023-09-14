CREATE TABLE analysis.dm_rfm_segments (
user_id bigint PRIMARY KEY,
recency smallint NOT NULL,
frequency smallint NOT NULL,
monetary smallint NOT NULL)
