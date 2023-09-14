import pyspark.sql.functions as F 
from pyspark.sql.types import DateType
from pyspark.sql.window import Window
from tools import get_spark_session, write_df
import sys

date = sys.argv[1]

spark = get_spark_session('dm_zone')


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

write_df(result, 'dm_zone', date)