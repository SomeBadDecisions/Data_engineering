# Потоковая обработка данных

## 1.1 Описание
В рамках данного проекта необходимо создать приложение для потоковой обработки данных. Заказчик - агрегатор доставки еды.
В приложении заказчика появилась новая функция - возможность оформления подписки. Подписка дает ряд возможностей, среди которых - добавление понравившегося ресторана в избранное.

От "избранных" ресторанов пользователи будут получать уведомления о различных акциях с ограниченным сроком действия. 

Система работает так:

- Ресторан отправляет через мобильное приложение акцию с ограниченным предложением. Например, такое: «Вот и новое блюдо — его нет в обычном меню. 
Дарим на него скидку 70% до 14:00! Нам важен каждый комментарий о новинке»;
- Сервис проверяет, у кого из пользователей ресторан находится в избранном списке;
- Сервис формирует заготовки для push-уведомлений этим пользователям о временных акциях. Уведомления будут отправляться только пока действует акция.

## 1.2 План проекта

Схема работы сервиса:
![2 _Proekt_1663599076](https://user-images.githubusercontent.com/63814959/236286217-d1d70bc0-9089-45d8-9f11-552d93efe7ee.png)

Для реализации необходимо:

**1. читать данные из Kafka с помощью Spark Structured Streaming и Python в режиме реального времени**

Пример входного сообщения:

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

где:

- **`"restaurant_id"`**  — UUID ресторана;
- **`"adv_campaign_id"`** — UUID рекламной кампании;
- **`"adv_campaign_content"`** — текст кампании;
- **`"adv_campaign_owner"`** — сотрудник ресторана, который является владельцем кампании;
- **`"adv_campaign_owner_contact"`** — его контакт;
- **`"adv_campaign_datetime_start"`** — время начала рекламной кампании в формате timestamp;
- **`"adv_campaign_datetime_end"`** — время её окончания в формате timestamp;
- **`"datetime_created"`** — время создания кампании в формате timestamp.

**2. получать список подписчиков из Postgres**

Таблица содержит соответствия пользователей и ресторанов, на которые они подписаны.

**DDL:**

```sql
CREATE TABLE public.subscribers_restaurants (
    id serial4 NOT NULL,
    client_id varchar NOT NULL,
    restaurant_id varchar NOT NULL,
    CONSTRAINT pk_id PRIMARY KEY (id)
);
```

**3. джойнить данные из Kafka с данными из БД**

**4. сохранять в памяти полученные данные, чтобы не собирать их заново после отправки в Postgres или Kafka**

**5.отправлять выходное сообщение в Kafka с информацией об акции, пользователе со списком избранного и ресторане, а ещё вставлять записи в Postgres, чтобы впоследствии получить фидбэк от пользователя.**

**DDL** целевой таблицы:

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

Структура выходного сообщения:

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

По сравнению с входным добавляются два новый поля:
- **`"client_id"`** — UUID подписчика ресторана, который достаётся из таблицы Postgres.
- **`"trigger_datetime_created"`** — время создания триггера, то есть выходного сообщения. Оно добавляется во время создания сообщения.

**6. На основе полученной информации сервис push-уведомлений будет читать сообщения из Kafka и формировать готовые уведомления.**

Этот шаг происходит уже без нашего участия.

## 2.1 Проверка работы потока

Для начала проверим корректность работы потока данных. Для этого отправим тестовое сообщение через **kcat** и прочитаем его.
Для этого напишем тестовые консьюмер и продьюсер:

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

Убеждаемся, что поток работает корректно, сообщения успешно поступают консьюмеру.

## 2.2 Читаем данные из Kafka 

Здесь и по всему проекту в дальнейшем **логин и пароль были изменены**.

Для начала прочитаем данные об акциях ресторанов из Kafka:

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

После десериализуем их, дополнительно рассчитаем текущее время, чтобы отбирать только актуальные предложения:

```python
# определяем схему входного сообщения для json
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

# определяем текущее время в UTC в миллисекундах
current_timestamp_utc = int(round(datetime.utcnow().timestamp()))

# десериализуем из value сообщения json и фильтруем по времени старта и окончания акции
filtered_read_stream_df = restaurant_read_stream_df \
    .select(from_json(col("value").cast("string"), schema).alias("result")) \
    .select(col("result.*")) \
    .where((col("adv_campaign_datetime_start") < current_timestamp_utc) & (col("adv_campaign_datetime_end") > current_timestamp_utc))
```

## 2.3 Читаем данные из postgres

Прочитаем статичные данные из postgres о подписках пользователей на рестораны:

```python 
#читаем из Postgres информацию о подписках пользователей на рестораны
subscribers_restaurant_df = spark.read \
                    .format('jdbc') \
                      .option('url', 'jdbc:postgresql://rc1a-fswjkpli01zafgjm.mdb.yandexcloud.net:6432/de') \
                    .option('driver', 'org.postgresql.Driver') \
                    .option('dbtable', 'subscribers_restaurants') \
                    .option('user', 'user') \
                    .option('password', 'password') \
                    .load()
```

Сджойним со стриминговыми данными:

```python 
# джойним данные из сообщения Kafka с пользователями подписки по restaurant_id (uuid). Добавляем время создания события.
result_df = filtered_read_stream_df.alias('stream') \
    .join(subscribers_restaurant_df.alias('static'), \
        col("stream.restaurant_id") == col("static.restaurant_id")) \
    .select(col("stream.*"), col("client_id")) \
    .withColumn("trigger_datetime_created", lit(int(round(datetime.utcnow().timestamp()))))
```

## 2.4 Запишем результат

Результат нам нужно отправлять в новый топик Kafka и записывать в postgres. Напишем функцию, которую впоследствии будем подавать в **.foreachBatch()**:

```python 
# метод для записи данных в 2 target: в PostgreSQL для фидбэков и в Kafka для триггеров
def foreach_batch_function(df, epoch_id):
    # сохраняем df в памяти, чтобы не создавать df заново перед отправкой в Kafka
    df.persist()
    # записываем df в PostgreSQL с полем feedback
    df.write \
      .mode('append') \
      .format('jdbc') \
      .option("url", "jdbc:postgresql://localhost:5432/de") \
      .option('driver', 'org.postgresql.Driver') \
      .option("dbtable", "subscribers_feedback") \
      .option("user", "user") \
      .option("password", "password") \
      .save()
    # создаём df для отправки в Kafka. Сериализация в json.
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
    # отправляем сообщения в результирующий топик Kafka без поля feedback
    kafka_df.write \
        .format('kafka') \
        .option('kafka.bootstrap.servers', 'rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091') \
        .option('kafka.security.protocol', 'SASL_SSL') \
        .option('kafka.sasl.jaas.config', 'org.apache.kafka.common.security.scram.ScramLoginModule required username=\"username\" password=\"password\";') \
        .option('kafka.sasl.mechanism', 'SCRAM-SHA-512') \
        .option('topic', 'konstantin_result') \
        .save()
    # очищаем память от df
    df.unpersist()
```

Запустим стриминг:

```python 
result_df.writeStream \
    .foreachBatch(foreach_batch_function) \
    .start() \
    .awaitTermination()
```

## 3 Итоги

В рамках данного проекта был реализован сервис оповещения клиентов об актуальных акциях ресторанов, на которые они подписаны.

1. Стриминговые данные прочитаны из Kafka, статичные из postgres
2. Данные обработаны и преобразованы
3. Итоги отправлены в выходной топик Kafka для последующей передачи приложению, напрявляющему push-уведомления клиентам. Дополнительно итоги записаны в postgres для последующей аналитики.

Итоговый код: **/src/rest_promo_streaming.py**