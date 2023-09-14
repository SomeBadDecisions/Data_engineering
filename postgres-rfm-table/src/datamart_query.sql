INSERT INTO analysis.dm_rfm_segments (
user_id,
recency,
frequency,
monetary)

SELECT 
r.user_id 
,r.recency
,f.frequency
,m.monetary_value
FROM analysis.tmp_rfm_recency r
inner join analysis.tmp_rfm_frequency f 
	on r.user_id = f.user_id
inner join analysis.tmp_rfm_monetary_value m
	on r.user_id = m.user_id ;



0	3	5	5
1	4	4	4
2	3	3	3
3	3	3	2
4	3	4	2
5	4	5	5
6	1	3	2
7	5	1	2
8	4	1	2
9	4	3	2
