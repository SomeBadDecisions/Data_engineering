# Data Lake design

## 1.1 Introduction
In this project I have to develop a Data Lake for a social network recommendation system.
Social network will suggest new friends to the user based on:

- same channel membership
- distance between users less than 1 kilometer
- users have never communicated before

Moreover the customer wants to study the audience of the social network in order to launch monetization in the future. 
For this reason he wants me to do some geoanalytics:

- find out where the majority of users are based on the number of messages, likes and subscriptions from one point
- define which Australian city has the biggest new user registration number
- figure out how often users travel and which cities they choose

This information will help customer to insert advertising into the social network: the application will be able to take into account the user’s location and offer him suitable services from partner companies.

I have source which contains coordinates of each event (message, subscription, reaction, registration)
Data stored in HDFS: **/user/master/data/geo/events/**

Also I have csv-file which contains coordinates of Australian city centers for comparasion. 
Stored at: **/src/geo.csv**

### 1.2 Data Lake architecture
I need 4 Data Lake layers for this task:

- **Raw** - for raw data. Directory: **/user/master/data/geo/events/date=YYYY-MM-dd**. Data partitioned by date;
- **ODS** - for preprocessed data. Directory: **/user/konstantin/data/events/event_type=XXX/date=yyyy-MM-dd**. Data partitioned by date and event type;
- **Sandbox** - for some Ad-hoc analytics and tests. Directory: **/user/konstantin/analytics/…**;
- **Data mart** - final versions of Data marts. Directory: **/user/konstantin/prod/…**.

### 1.3 Target data marts

#### 1.3.1 User data mart

User data mart should contain:

- **user_id** ;
- **act_city** — current address. The city from which the last message was sent;
- **home_city** — home address. The last city the user was in for longer than 27 days;
- **travel_count** — number of cities visited. If the user has visited a certain city again, this is considered a separate visit.;
- **travel_array** — list of cities in order of visiting;
- **local_time** — local time of event.

#### 1.3.2 Zone data mart

Zone (or city) data mart should contain events in a specific city for the week and month:

- **month** — month of calculation;
- **week** — week of calculation;
- **zone_id** — zone (city) id;
- **week_message** — number of messages per week;
- **week_reaction** — number of reactions per week;
- **week_subscription** — number of subscriptions per week;
- **week_user** — number of new user registrations per week;
- **month_message** — number of messages per month;
- **month_reaction** — number of reactions per month;
- **month_subscription** — number pf subscriptions per month;
- **month_user** — number of new user registrations per month.

#### 1.3.3 Friend recommendation data mart

Friend recommendation data mart should contain:

- **user_left** — first user;
- **user_right** — second user;
- **processed_dttm**;
- **zone_id**;
- **local_time**.

## 2 Data mart development

At first I have to fill in ODS-layer. 

I want data to be partitioned not only by date but also for event type:

```python
events = spark.read.parquet("/user/master/data/geo/events")

events.write.partitionBy("event_type","date")\
.mode("overwrite").parquet("/user/konstantin/data/events")
```

### 2.1 User data mart development

At first I have to determine in which city the took place.

I have file **geo.csv** with city centers coordinations. I'm adding aditional field with timezone names. It'll help me to determine **local_time**.

Updated file : **/src/geo_timezone.csv** 

In this task I have to use the formula for the distance between two points on a sphere:

![formula](https://github.com/SomeBadDecisions/Data_engineering/assets/63814959/050ce635-1ff7-46af-b3bb-005ea38d4d1a)

Where:

- $ \varphi $ 1 - latitude of the first point 
- $\varphi$ 2 - latitude of the second point
- $\lambda$ 1 - longtitude of the first point
- $\lambda$ 2 - longtitude of the second point
- $r$ - radius of the Earth approximatelu equal to 6371 kilometers

In both the data and the **geo.csv**, latitude and longitude are indicated in degrees. For my Data Lake I need to convert the values to radians.
So I'm loading and then reading a new csv-file with city coordinates into HDFS:

```console
hdfs dfs -copyFromLocal geo_timezone.csv /user/konstantin/data/
```

```python 
geo = spark.read.csv(geo_path, sep=';', header= True)\
      .withColumnRenamed("lat", "city_lat")\
      .withColumnRenamed("lng", "city_lon")
```
Then I'm reading raw data:

```python
events_geo = spark.read.parquet(events_path) \
    .where("event_type == 'message'")\
    .withColumnRenamed("lat", "msg_lat")\
    .withColumnRenamed("lon","msg_lon")\
    .withColumn('user_id', F.col('event.message_from'))\
    .withColumn('event_id', F.monotonically_increasing_id())
```
Final data structure:

![schema](https://user-images.githubusercontent.com/63814959/226731775-9290acd7-13ad-4c16-a851-ae6260547961.png)

Then I have to write a function to determine the real city for each event. 

<details><summary>In this function I will use the distance formula given above:</summary>

```python
def get_city(events_geo, geo):

    EARTH_R = 6371

    calculate_diff = 2 * F.lit(EARTH_R) * F.asin(
            F.sqrt(
                F.pow(F.sin((F.radians(F.col("msg_lat")) - F.radians(F.col("city_lat"))) / 2), 2) +
                F.cos(F.radians(F.col("msg_lat"))) * F.cos(F.radians(F.col("city_lat"))) *
                F.pow(F.sin((F.radians(F.col("msg_lon")) - F.radians(F.col("city_lon"))) / 2), 2)
            )
        )

    window = Window().partitionBy('user_id').orderBy(F.col('diff').asc())
    events = events_geo \
            .crossJoin(geo) \
            .withColumn('diff', calculate_diff) \
            .withColumn("row_number", F.row_number().over(window)) \
            .filter(F.col('row_number')==1) \
            .drop('row_number') 
    

    return events
	
	
events = get_city(
    events_geo=events_geo,
    geo=geo
)
```
</details>


Next I have to find the current address. Current address is the city in which the user was located when the last message was sent:

```python
window_act_city = Window().partitionBy('user_id').orderBy(F.col("date").desc())
act_city = events \
            .withColumn("row_number", F.row_number().over(window_act_city)) \
            .filter(F.col("row_number")==1) \
            .withColumnRenamed('city', 'act_city')
```

Then I'm making a list of continuous stay in one city based on the date of the message from the next city.

<details><summary>Based on this information, I'm finding a list of cities visited, their number and home city:</summary>

```python
window = Window.partitionBy('user_id').orderBy('date')

travels = events \
            .withColumn('pre_city', F.lag('city').over(window)) \
            .withColumn('series', F.when(F.col('city') == F.col('pre_city'), F.lit(0)).otherwise(F.lit(1))) \
            .select('user_id', 'date', 'city', 'pre_city', 'series') \
            .withColumn('sum_series', F.sum('series').over(window)) \
            .groupBy('sum_series', 'user_id', 'city').agg(F.min('date').alias('date')) \
            .drop('sum_series')
			
travels_array = travels \
            .groupBy("user_id") \
    .agg(F.collect_list('city').alias('travel_array')) \
    .select('user_id', 'travel_array', F.size('travel_array').alias('travel_count'))
	
window = Window.partitionBy('user_id').orderBy('date')
window_desc = Window.partitionBy('user_id').orderBy(F.col('date').desc())

home = travels \
        .withColumn('next_date', F.lead('date').over(window)) \
        .withColumn('days_staying', F.when(F.col('next_date').isNull(), '1') \
        .otherwise(F.datediff(F.col('next_date'), F.col('date')))) \
        .filter('days_staying > 27') \
        .withColumn('rn', F.row_number().over(window_desc)) \
        .filter('rn == 1') \
        .drop('rn') \
        .withColumnRenamed('city', 'home_city')
		
```

</details>

Then I have to write function to determine the local time of current city:

```python
def calc_local_tm(events):    
    return events.withColumn("TIME",F.col("event.datetime").cast("Timestamp"))\
        .withColumn("local_time",F.from_utc_timestamp(F.col("TIME"),F.col('timezone')))\
        .select("local_time", 'user_id')
		
local_time = calc_local_tm(act_city)
```

All the necessary data is ready, all that remains is to collect the final result:

```python
result = events \
        .join(act_city, ['user_id'], 'left') \
        .join(travels_array,['user_id'], 'left') \
        .join(home, ['user_id'], 'left') \
        .join(local_time, ['user_id'], 'left') \
        .selectExpr('user_id', 'act_city', 'home_city', 'travel_count', 'travel_array', 'local_time') \
        .distinct()
```
Final data mart sample:


![image](https://user-images.githubusercontent.com/63814959/226732751-ca4aa9a4-85fc-47fd-ba32-e2b1c20059b5.png)


To build the final spark job I need 2 functions: the first one shoulkd initialize spark session and the second one should write the result to data lake.

Function for initializing a spark session (in general terms, since the cluster parameters are not known):

```python
def spark_session_init(name):
    return SparkSession \
        .builder \
        .master("yarn")\
        .appName(f"{name}") \
        .getOrCreate()
```

Write function:

```python 
def write_df(df, df_name, date):
    df.write.mode('overwrite').parquet(f'/user/konstantin/prod/{df_name}/date={date}')
```

I stored this functions in separate file: **/src/scripts/tools.py**

Final job: **/src/scripts/dm_users.py** 

Local **.ipynb**: **/src/dm_users.ipynb** 

### 2.2 Zone data mart development

I'm creating a data mart with the distribution of various events by city.
I need this to understand user behavior depending on the geographical area.

At first I need to read data.
This step is the same as with user data mart, except I'm selecting all events, and not just those with the “message” type.

The data does not include events with the “registration” type, so I have to collect all other types first:

```python
w_week = Window.partitionBy(['city', F.trunc(F.col("date"), "week")])
w_month = Window.partitionBy(['city', F.trunc(F.col("date"), "month")])

pre_result = events.withColumn('week_message', F.count(F.when(events.event_type == 'message','event_id')).over(w_week)) \
    .withColumn('week_reaction', F.count(F.when(events.event_type == 'reaction','event_id')).over(w_week)) \
    .withColumn('week_subscription', F.count(F.when(events.event_type == 'subscription','event_id')).over(w_week)) \
    .withColumn('month_message', F.count(F.when(events.event_type == 'message','event_id')).over(w_month)) \
    .withColumn('month_reaction', F.count(F.when(events.event_type == 'reaction','event_id')).over(w_month)) \
    .withColumn('month_subscription', F.count(F.when(events.event_type == 'subscription','event_id')).over(w_month)) \
    .withColumn('month', F.trunc(F.col("date"), "month")) \
    .withColumn('week', F.trunc(F.col("date"), "week")) \
    .selectExpr('month', 'week', 'id as zone_id', 'week_message', 'week_reaction', 'week_subscription', \
            'month_message', 'month_reaction', 'month_subscription') \
    .distinct()
```

<details><summary>Next I'm calculating the date of the first event for each user and joining with data from precious step</summary>

```python
window = Window.partitionBy('user_id').orderBy(F.col('date'))
reg = events \
        .withColumn('min_date', F.min('date').over(window)) \
        .withColumn('first_city', F.first('id').over(window)) \
        .withColumn('week', F.trunc(F.col("min_date"), "week")) \
        .withColumn('month', F.trunc(F.col("min_date"), "month")) \
        .selectExpr("user_id", "first_city",  "min_date", "week", "month") \
        .distinct() 
		
r_week = Window.partitionBy(['first_city', "week"])
r_month = Window.partitionBy(['first_city', "month"])

reg_agg = reg \
        .withColumn('week_user', F.count('user_id').over(r_week)) \
        .withColumn('month_user', F.count('user_id').over(r_month)) \
        .selectExpr('month', 'week', 'month_user', 'week_user', 'first_city as zone_id') \
        .distinct()
		
result = pre_result.join(reg_agg, ['week', 'month', 'zone_id'], 'left') \
        .select(pre_result['month'], 'week', 'zone_id', 'week_message', 'week_reaction', 'week_subscription', 'week_user', \
            'month_message', 'month_reaction', 'month_subscription', 'month_user')
```

</details>

Final job: **/src/scripts/dm_zone.py**

Local **.ipynb**: **/src/dm_zone.ipynb** 

### 2.3 Friend recommendation data mart development

In the final data mart I have to collect paired attributes (two IDs) of users, which can be recommended to each other.

At first, I'm reading the data and finding the coordinates of the last message sent for each user:

```python 
window_last_msg = Window.partitionBy('user_id').orderBy(F.col('event.message_ts').desc())
last_msg = events.where("event_type == 'message'") \
    .where('msg_lon is not null') \
    .withColumn("rn",F.row_number().over(window_last)) \
    .filter(F.col('rn') == 1) \
    .drop(F.col('rn')) \
    .selectExpr('user_id', 'msg_lon as lon', 'msg_lat as lat', 'city', 'event.datetime as datetime', 'timezone')
```

Then I'm collecting a list of users and channels to which they are subscribed using self-join:

```python 
user_channel = events_geo.select(
    F.col('event.subscription_channel').alias('channel'),
    F.col('event.user').alias('user_id')
).distinct()

user_channel_f = user_channel \
            .join(user_channel.withColumnRenamed('user_id', 'user_id_2'), ['channel'], 'inner') \
            .filter('user_id < user_id_2')
```

Next I'm finding current coordinates for each pair of users:

```python 
channel_msg = last_msg \
              .join(user_channel_f, ['user_id'], 'inner') \
              .withColumnRenamed("lon", "user_1_lon") \
              .withColumnRenamed("lat", "user_1_lat") \
              .withColumnRenamed("city", "city_1") \
              .withColumnRenamed("datetime", "datetime_1") \
              .withColumnRenamed("timezone", "timezone_1") \
              .join(last_msg.withColumnRenamed("user_id", "user_id_2"), ["user_id_2"], "inner") \
              .withColumnRenamed("lon", "user_2_lon") \
              .withColumnRenamed("lat", "user_2_lat") \
              .withColumnRenamed("city", "city_2") \
              .withColumnRenamed("datetime", "datetime_2") \
              .withColumnRenamed("timezone", "timezone_2")
```

Using the distance formula I'm determing the distance between users and then filtering it by <= 1 kilometer>:

```python 
distance = channel_msg \
    .withColumn('pre_lon', F.radians(F.col('user_1_lon')) - F.radians(F.col('user_2_lon'))) \
    .withColumn('pre_lat', F.radians(F.col('user_1_lat')) - F. radians(F.col('user_2_lat'))) \
    .withColumn('dist', F.asin(F.sqrt(
        F.sin(F.col('pre_lat') / 2) ** 2 + F.cos(F.radians(F.col('user_2_lat')))
        * F.cos(F.radians(F.col('user_1_lat'))) * F.sin(F.col('pre_lon') / 2) ** 2
    )) * 2 * 6371) \
    .filter(F.col('dist') <= 1) \
    .withColumn("TIME",F.col("datetime_1").cast("Timestamp"))\
    .withColumn("local_time",F.from_utc_timestamp(F.col("TIME"),F.col('timezone_1')))
```

After that I'm collecting the list of users who have already written to each other:

```python 
events_pairs = events.selectExpr('event.message_from as user_id','event.message_to as user_id_2') \
               .where(F.col('user_id_2').isNotNull())
			   
events_pairs_union = events_pairs.union(events_pairs.select('user_id_2', 'user_id')).distinct()
```

Finally I'm using **left-anti** join to exclude users who have already written to each other:

```python 
result = distance \
    .join(events_pairs_union, ['user_id', 'user_id_2'], 'left_anti') \
    .withColumn('processed_dttm', F.current_timestamp()) \
    .selectExpr('user_id as user_left', 'user_id_2 as user_right', 'processed_dttm', 'city_1 as city', 'local_time') \
	.distinct()
```

Final job: **/src/scripts/dm_rec.py**

Local **.ipynb**: **/src/dm_rec.ipynb** 

### 2.4 Airflow automation

In order for data marts to be calculated daily, I write a DAG and set it to daily schedule in Airflow.

DAG: **/src/dags/dag_social_rec.py**

## Conclusion

In this project were developed:

- Data Lake with 4 layers (raw, ODS, sandbox and data mart) 
- 3 pyspark jobs for data marts creation  
- DAG as a daily update tool

All pyspark jobs: **/src/scripts**

Final DAG: **/src/dags**

Local **Jupyter**-notebooks: **/src**
