# Stream processing

## 1.1 Project description

Within this project I have to create stream processing app for food delivery aggregator.
A new feature has appeared in the customer's application - now users can subscribe to restaurants. 
Subscription provides a number of opportunities, including adding restaurant to your favorites.
After that users will receive notifications from favorite restaurants about various limited-time promotions.

The system works like this:

- The restaurant sends a limited offer promotion through its mobile app. For example, something like this: “Here’s a new dish - it’s not on the regular menu.
We are giving a 70% discount on it until 2 pm! Every comment about the new product is important to us”;
- The service checks which user has the restaurant in their favorite list;
- The service generates templates for push notifications to these users about temporary promotions. Notifications will only be sent while the promotion is valid.

## 1.2 Project structure 

Pipeline scheme:
![stream_processing](https://github.com/SomeBadDecisions/Data_engineering/assets/63814959/7ee04ca8-366d-418b-9ba1-31185e4e37b5)


Realization steps:

**1. reading data in real time from Kafka with Spark Structured Streaming and Python**

Input message example:

```json
{
  "restaurant_id": "123e4567-e89b-12d3-a456-426614174000",
  "adv_campaign_id": "123e4567-e89b-12d3-a456-426614174003",
  "adv_campaign_content": "first campaign",
  "adv_campaign_owner": "Ivanov Ivan Ivanovich",
  "adv_campaign_owner_contact": "iiivanov@restaurant.ru",
  "adv_campaign_datetime_start": 1659203516,
  "adv_campaign_datetime_end": 2659207116,
  "datetime_created": 1659131516
}
```

- **`"restaurant_id"`**  — restaurant UUID;
- **`"adv_campaign_id"`** — promotion UUID;
- **`"adv_campaign_content"`** — promotion content;
- **`"adv_campaign_owner"`** — restaurant employee responsible for campaign;
- **`"adv_campaign_owner_contact"`** — restaurant's employee contact;
- **`"adv_campaign_datetime_start"`** — start time of the advertising campaign in the timestamp format;
- **`"adv_campaign_datetime_end"`** — end time of the advertising campaign in the timestamp format;
- **`"datetime_created"`** — campaing creation date and time in the timestamp format.

**2. Reading data about restaurant subscribers from Postgres**

The table contains matches between users and restaurants they follow.

**DDL:**

```sql
CREATE TABLE public.subscribers_restaurants (
    id serial4 NOT NULL,
    client_id varchar NOT NULL,
    restaurant_id varchar NOT NULL,
    CONSTRAINT pk_id PRIMARY KEY (id)
);
```

**3. Joining data from Kafka with data from the Postgres**

**4. Persisting data, so as not to create them again after sending them to Postgres or Kafka**

**5. Sending an output message with information about the promotion, the user with a list of favorites and the restaurant to Kafka. And also inserting data into Postgres for future feedback from the user.**

Target table **DDL**:

```sql 
CREATE TABLE public.subscribers_feedback (
  id serial4 NOT NULL,
    restaurant_id text NOT NULL,
    adv_campaign_id text NOT NULL,
    adv_campaign_content text NOT NULL,
    adv_campaign_owner text NOT NULL,
    adv_campaign_owner_contact text NOT NULL,
    adv_campaign_datetime_start int8 NOT NULL,
    adv_campaign_datetime_end int8 NOT NULL,
    datetime_created int8 NOT NULL,
    client_id text NOT NULL,
    trigger_datetime_created int4 NOT NULL,
    feedback varchar NULL,
    CONSTRAINT id_pk PRIMARY KEY (id)
);
```

Output message example:

```json
{
  "restaurant_id":"123e4567-e89b-12d3-a456-426614174000",
  "adv_campaign_id":"123e4567-e89b-12d3-a456-426614174003",
  "adv_campaign_content":"first campaign",
  "adv_campaign_owner":"Ivanov Ivan Ivanovich",
  "adv_campaign_owner_contact":"iiivanov@restaurant.ru",
  "adv_campaign_datetime_start":1659203516,
  "adv_campaign_datetime_end":2659207116,
  "client_id":"023e4567-e89b-12d3-a456-426614174000",
  "datetime_created":1659131516,
  "trigger_datetime_created":1659304828
}
```

Compared to input message I add two fields:
- **`"client_id"`** — restaurant's subscriber UUID from Postgres.
- **`"trigger_datetime_created"`** — output message time.

**6. Based on the information received, the push notification service reading messages from Kafka and generating notifications.**

This step takes place without my participation

## 2.1 Streaming checking

At first I send and then read test message with **kcat**. For this purpose I create test producer and consumer: 
Consumer:

```bash
!kafkacat -b rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091 \
-X security.protocol=SASL_SSL \
-X sasl.mechanisms=SCRAM-SHA-512 \
-X sasl.username="username" \
-X sasl.password="password" \
-X ssl.ca.location=/root/CA.pem \
-t konstantin \
-K: \
-C
```

Producer:

```bash
!kafkacat -b rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091 \
-X security.protocol=SASL_SSL \
-X sasl.mechanisms=SCRAM-SHA-512 \
-X sasl.username="username" \
-X sasl.password="password" \
-X ssl.ca.location=/root/CA.pem \
-t konstantin \
-K: \
-P
key:{"restaurant_id": "123e4567-e89b-12d3-a456-426614174000","adv_campaign_id": "123e4567-e89b-12d3-a456-426614174003","adv_campaign_content": "first campaign","adv_campaign_owner": "Ivanov Ivan Ivanovich","adv_campaign_owner_contact": "iiivanov@restaurant_id","adv_campaign_datetime_start": 1659203516,"adv_campaign_datetime_end": 2659207116,"datetime_created": 1659131516}
```

## 2.2 Читаем данные из Kafka 

For security reasons I changed **login and password**.

Read data from Kafka:

```python
restaurant_read_stream_df = spark.readStream \
    .format('kafka') \
    .option('kafka.bootstrap.servers', 'rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091') \
    .option('kafka.security.protocol', 'SASL_SSL') \
    .option('kafka.sasl.jaas.config', 'org.apache.kafka.common.security.scram.ScramLoginModule required username=\"username\" password=\"password\";') \
    .option('kafka.sasl.mechanism', 'SCRAM-SHA-512') \
    .option('subscribe', 'konstantin_in') \
    .load()
```

Deserialize it and then filter by relevance:

```python
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

current_timestamp_utc = int(round(datetime.utcnow().timestamp()))

filtered_read_stream_df = restaurant_read_stream_df \
    .select(from_json(col("value").cast("string"), schema).alias("result")) \
    .select(col("result.*")) \
    .where((col("adv_campaign_datetime_start") < current_timestamp_utc) & (col("adv_campaign_datetime_end") > current_timestamp_utc))
```

## 2.3 Read data from Postgres

Read subscribtions static data from Postgres:

```python 
subscribers_restaurant_df = spark.read \
                    .format('jdbc') \
                      .option('url', 'jdbc:postgresql://rc1a-fswjkpli01zafgjm.mdb.yandexcloud.net:6432/de') \
                    .option('driver', 'org.postgresql.Driver') \
                    .option('dbtable', 'subscribers_restaurants') \
                    .option('user', 'user') \
                    .option('password', 'password') \
                    .load()
```

Join with streaming data:

```python 
result_df = filtered_read_stream_df.alias('stream') \
    .join(subscribers_restaurant_df.alias('static'), \
        col("stream.restaurant_id") == col("static.restaurant_id")) \
    .select(col("stream.*"), col("client_id")) \
    .withColumn("trigger_datetime_created", lit(int(round(datetime.utcnow().timestamp()))))
```

## 2.4 Write result

I have to send result to new Kafka topic and Postgres. 

<details><summary>I write python function for this purpose which I will use later in <strong>.foreachBatch()</strong></summary>

```python 
def foreach_batch_function(df, epoch_id):
    df.persist()
    df.write \
      .mode('append') \
      .format('jdbc') \
      .option("url", "jdbc:postgresql://localhost:5432/de") \
      .option('driver', 'org.postgresql.Driver') \
      .option("dbtable", "subscribers_feedback") \
      .option("user", "user") \
      .option("password", "password") \
      .save()
	  
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

    kafka_df.write \
        .format('kafka') \
        .option('kafka.bootstrap.servers', 'rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091') \
        .option('kafka.security.protocol', 'SASL_SSL') \
        .option('kafka.sasl.jaas.config', 'org.apache.kafka.common.security.scram.ScramLoginModule required username=\"username\" password=\"password\";') \
        .option('kafka.sasl.mechanism', 'SCRAM-SHA-512') \
        .option('topic', 'konstantin_result') \
        .save()

    df.unpersist()
```

</details>

Start streaming:

```python 
result_df.writeStream \
    .foreachBatch(foreach_batch_function) \
    .start() \
    .awaitTermination()
```

## 3 Conclusion

Within this project I created service which sends relevant push-notifications to restaurant subscribers. 

1. Static data was read from Postgres and streaming data was read from Kafka
2. Data was processed and transformed
3. Results were send to push-notification service through Kafka. 
4. Also results were stored in Postgres fo future analysis and users feedback.

Final code: **/src/rest_promo_streaming.py**