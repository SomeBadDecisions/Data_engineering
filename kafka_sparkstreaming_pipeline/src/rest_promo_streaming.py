import os

from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, col, lit, struct
from pyspark.sql.types import StructType, StructField, StringType, LongType

# method for writing data to 2 targets: PostgreSQL for feedback and Kafka for triggers
def foreach_batch_function(df, epoch_id):
    # persisting df so as not to create again before sending to Kafka
    df.persist()
    # writing df to Postgres with feedback field
    df.write \
      .mode('append') \
      .format('jdbc') \
      .option("url", "jdbc:postgresql://localhost:5432/de") \
      .option('driver', 'org.postgresql.Driver') \
      .option("dbtable", "subscribers_feedback") \
      .option("user", "user") \
      .option("password", "user") \
      .save()
    # creating df for sending to Kafka. Serializing in json.
    kafka_df = df.select(to_json( \
            struct("restaurant_id", \
                   "adv_campaign_id", \
                   "adv_campaign_content", \
                   "adv_campaign_owner", \
                   "adv_campaign_owner_contact", \
                   "adv_campaign_datetime_start", \
                   "adv_campaign_datetime_end", \
                   "client_id", \
                   "datetime_created", \
                   "trigger_datetime_created")) \
            .alias("value"))
    # sending messages in target Kafka topic without feedback field
    kafka_df.write \
        .format('kafka') \
        .option('kafka.bootstrap.servers', 'rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091') \
        .option('kafka.security.protocol', 'SASL_SSL') \
        .option('kafka.sasl.jaas.config', 'org.apache.kafka.common.security.scram.ScramLoginModule required username=\"username\" password=\"password\";') \
        .option('kafka.sasl.mechanism', 'SCRAM-SHA-512') \
        .option('topic', 'konstantin_result') \
        .save()
    # unpersisting df
    df.unpersist()

# essential libs for Spark integration with Kafka and PostgreSQL
spark_jars_packages = ",".join(
        [
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0",
            "org.postgresql:postgresql:42.4.0",
        ]
    )

# creating spark session with libs in spark_jars_packages for integration with Postgres and Kafka
spark = SparkSession.builder \
    .appName("RestaurantSubscribeStreamingService") \
    .config("spark.sql.session.timeZone", "UTC") \
    .config("spark.jars.packages", spark_jars_packages) \
    .getOrCreate()

# reading from Kafka topic messages with promotions from restaurants 
restaurant_read_stream_df = spark.readStream \
    .format('kafka') \
    .option('kafka.bootstrap.servers', 'rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091') \
    .option('kafka.security.protocol', 'SASL_SSL') \
    .option('kafka.sasl.jaas.config', 'org.apache.kafka.common.security.scram.ScramLoginModule required username=\"username\" password=\"password\";') \
    .option('kafka.sasl.mechanism', 'SCRAM-SHA-512') \
    .option('subscribe', 'konstantin_in') \
    .load()

# defining input message scheme for json
schema = StructType([
    StructField("restaurant_id", StringType()),
    StructField("adv_campaign_id", StringType()),
    StructField("adv_campaign_content", StringType()),
    StructField("adv_campaign_owner", StringType()),
    StructField("adv_campaign_owner_contact", StringType()),
    StructField("adv_campaign_datetime_start", LongType()),
    StructField("adv_campaign_datetime_end", LongType()),
    StructField("datetime_created", LongType()),
])

# defining current UTC time
current_timestamp_utc = int(round(datetime.utcnow().timestamp()))

# deserializing from value and filtering by promotion start and end time
filtered_read_stream_df = restaurant_read_stream_df \
    .select(from_json(col("value").cast("string"), schema).alias("result")) \
    .select(col("result.*")) \
    .where((col("adv_campaign_datetime_start") < current_timestamp_utc) & (col("adv_campaign_datetime_end") > current_timestamp_utc))

#reading information about user's subscriptions on restaurants from Postgres
subscribers_restaurant_df = spark.read \
                    .format('jdbc') \
                      .option('url', 'jdbc:postgresql://rc1a-fswjkpli01zafgjm.mdb.yandexcloud.net:6432/de') \
                    .option('driver', 'org.postgresql.Driver') \
                    .option('dbtable', 'subscribers_restaurants') \
                    .option('user', 'user') \
                    .option('password', 'password') \
                    .load()

# Joining data from Kafka messages with user's subscriptions on restaurant_id (uuid). Adding event creation time field.
result_df = filtered_read_stream_df.alias('stream') \
    .join(subscribers_restaurant_df.alias('static'), \
        col("stream.restaurant_id") == col("static.restaurant_id")) \
    .select(col("stream.*"), col("client_id")) \
    .withColumn("trigger_datetime_created", lit(int(round(datetime.utcnow().timestamp()))))

# starting streaming
result_df.writeStream \
    .foreachBatch(foreach_batch_function) \
    .start() \
    .awaitTermination()