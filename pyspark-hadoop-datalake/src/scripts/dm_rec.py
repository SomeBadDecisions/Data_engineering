import pyspark.sql.functions as F 
from pyspark.sql.types import DateType
from pyspark.sql.window import Window
from tools import get_spark_session, write_df
import sys

date = sys.argv[1]

spark = get_spark_session('dm_users')


events_path = "/user/master/data/geo/events" 
geo_path = "/user/konstantin/data/geo_timezone.csv"

events_geo = spark.read.parquet(events_path) \
    .withColumnRenamed("lat", "msg_lat")\
    .withColumnRenamed("lon","msg_lon")\
    .withColumn('user_id', F.col('event.message_from'))\
    .withColumn('event_id', F.monotonically_increasing_id())

geo = spark.read.csv(geo_path, sep=';', header= True)\
      .withColumnRenamed("lat", "city_lat")\
      .withColumnRenamed("lng", "city_lon")

def get_city(events_geo, geo):

    EARTH_R = 6371

    calculate_diff = 2 * F.lit(EARTH_R) * F.asin(
            F.sqrt(
                F.pow(F.sin((F.radians(F.col("msg_lat")) - F.radians(F.col("city_lat"))) / 2), 2) +
                F.cos(F.radians(F.col("msg_lat"))) * F.cos(F.radians(F.col("city_lat"))) *
                F.pow(F.sin((F.radians(F.col("msg_lon")) - F.radians(F.col("city_lon"))) / 2), 2)
            )
        )

    window = Window().partitionBy('event_id').orderBy(F.col('diff').asc())
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

window_last_msg = Window.partitionBy('user_id').orderBy(F.col('event.message_ts').desc())
last_msg = events.where("event_type == 'message'") \
    .where('msg_lon is not null') \
    .withColumn("rn",F.row_number().over(window_last_msg)) \
    .filter(F.col('rn') == 1) \
    .drop(F.col('rn')) \
    .selectExpr('user_id', 'msg_lon as lon', 'msg_lat as lat', 'city', 'event.datetime as datetime', 'timezone')

user_channel = events_geo.select(
    F.col('event.subscription_channel').alias('channel'),
    F.col('event.user').alias('user_id')
).distinct()

user_channel_f = user_channel \
            .join(user_channel.withColumnRenamed('user_id', 'user_id_2'), ['channel'], 'inner') \
            .filter('user_id < user_id_2')

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

events_pairs = events.selectExpr('event.message_from as user_id','event.message_to as user_id_2') \
               .where(F.col('user_id_2').isNotNull())

events_pairs_union = events_pairs.union(events_pairs.select('user_id_2', 'user_id')).distinct()

result = distance \
    .join(events_pairs_union, ['user_id', 'user_id_2'], 'left_anti') \
    .withColumn('processed_dttm', F.current_timestamp()) \
    .selectExpr('user_id as user_left', 'user_id_2 as user_right', 'processed_dttm', 'city_1 as city', 'local_time') \
    .distinct()

write_df(result, 'dm_rec', date)