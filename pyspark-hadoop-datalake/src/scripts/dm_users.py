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
    .where("event_type == 'message'")\
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

window_act_city = Window().partitionBy('user_id').orderBy(F.col("date").desc())
act_city = events \
            .withColumn("row_number", F.row_number().over(window_act_city)) \
            .filter(F.col("row_number")==1) \
            .withColumnRenamed('city', 'act_city')

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

def calc_local_tm(events):    
    return events.withColumn("TIME",F.col("event.datetime").cast("Timestamp"))\
        .withColumn("local_time",F.from_utc_timestamp(F.col("TIME"),F.col('timezone')))\
        .select("local_time", 'user_id')

local_time = calc_local_tm(act_city)

result = events \
        .join(act_city, ['user_id'], 'left') \
        .join(travels_array,['user_id'], 'left') \
        .join(home, ['user_id'], 'left') \
        .join(local_time, ['user_id'], 'left') \
        .selectExpr('user_id', 'act_city', 'home_city', 'travel_count', 'travel_array', 'local_time') \
        .distinct()

write_df(result, 'dm_users', date)