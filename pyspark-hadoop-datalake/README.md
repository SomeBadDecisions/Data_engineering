# Проектирование Data Lake

## 1.1 Описание
В рамках данного проекта необходимо разработать Data Lake для системы рекомендаций социальной сети.
Приложение будет предлагать пользователю написать человеку, если пользователь и адресат:

- состоят в одном канале
- раньше никогда не переписывались
- находятся не дальше 1 км друг от друга

При этом заказчик хочет лучше изучить аудиторию соцсети, чтобы в будущем запустить монетизацию. Для этого было решено провести геоаналитику:

- выяснить, где находится большинство пользователей по количеству сообщений, лайков и подписок из одной точки
- посмотреть, в какой точке Австралии регистрируется больше всего новых пользователей
- определить, как часто пользователи путешествуют и какие города выбирают

Благодаря такой аналитике в соцсеть можно будет вставить рекламу: приложение сможет учитывать местонахождение пользователя и предлагать тому подходящие услуги компаний-партнёров. 

На источнике есть информация о координатах каждого события (сообщение, подписока, реакция, регистрация). 
Данные находятся в hdfs: **/user/master/data/geo/events/**

Также, есть csv-файл с координатами центров городов Австралии для сопоставления. Находится в директории **/src/geo.csv**

### 1.2 Структура хранилища
Для данной задачи будет достаточно 4 слоев:

- **Raw** - здесь хранятся сырые данные. Директория: **/user/master/data/geo/events/date=YYYY-MM-dd**. Данные партиционированы по датам;
- **ODS** - здесь будут хранится предобработанные данные. Директория: **/user/konstantin/data/events/event_type=XXX/date=yyyy-MM-dd**. Здесь данные дополнительно партиционированы по типу события;
- **Sandbox** - этот слой нужен для аналитики и решения Ad hoc задач. Директория: **/user/konstantin/analytics/…**;
- **Data mart** - на этом слое будут хранится итоговые версии витрин данных. Директория: **/user/konstantin/prod/…**.

### 1.3 Целевые витрины

#### 1.3.1 Витрина в разрезе пользователей

Витрина должна содержать следующие поля:

- **user_id** — идентификатор пользователя;
- **act_city** — актуальный адрес. Это город, из которого было отправлено последнее сообщение;
- **home_city** — домашний адрес. Это последний город, в котором пользователь был дольше 27 дней;
- **travel_count** — количество посещённых городов. Если пользователь побывал в каком-то городе повторно, то это считается за отдельное посещение;
- **travel_array** — список городов в порядке посещения;
- **local_time** — местное время события.

#### 1.3.2 Витрина в разрезе зон

В данной витрине должны содержаться события в конкретном городе за неделю и месяц:

- **month** — месяц расчёта;
- **week** — неделя расчёта;
- **zone_id** — идентификатор зоны (города);
- **week_message** — количество сообщений за неделю;
- **week_reaction** — количество реакций за неделю;
- **week_subscription** — количество подписок за неделю;
- **week_user** — количество регистраций за неделю;
- **month_message** — количество сообщений за месяц;
- **month_reaction** — количество реакций за месяц;
- **month_subscription** — количество подписок за месяц;
- **month_user** — количество регистраций за месяц.

#### 1.3.3 Витрина для рекомендации друзей

Данная витрина будет использоваться для рекомендации друзей пользователям. 

Поля витрины:

- **user_left** — первый пользователь;
- **user_right** — второй пользователь;
- **processed_dttm** — дата расчёта витрины;
- **zone_id** — идентификатор зоны (города);
- **local_time** — локальное время.

## 2 Построение витрин

Перед построением витрин заполним ODS-слой. Добавим партиционирование исходных данных по event_type:

```python
events = spark.read.parquet("/user/master/data/geo/events")

events.write.partitionBy("event_type","date")\
.mode("overwrite").parquet("/user/konstantin/data/events")
```

### 2.1 Витрина в разрезе пользователей 

Прежде всего необходимо определить, в каком городе было совершено событие.

У нас есть файл geo.csv с координатами центров городов. Дополнительно добавим туда названия таймзон для последующего определения **local_time**.
Обновленный файл : **/src/geo_timezone.csv** 

Для решения задачи воспользуемся формулой расстояния между двумя точками на сфере:

![image](https://user-images.githubusercontent.com/63814959/226187846-93b00a04-d831-4552-9120-1f18e40516e8.png)

И в данных, и в справочнике широта и долгота указана в градусах. Для этой задачи необходимо перевести значения в радианы.
Загрузим справочник с координатами городов в HDFS и прочиатем его:

```console
hdfs dfs -copyFromLocal geo_timezone.csv /user/konstantin/data/
```

```python 
geo = spark.read.csv(geo_path, sep=';', header= True)\
      .withColumnRenamed("lat", "city_lat")\
      .withColumnRenamed("lng", "city_lon")
```
Далее прочитаем сами данные:

```python
events_geo = spark.read.parquet(events_path) \
    .where("event_type == 'message'")\
    .withColumnRenamed("lat", "msg_lat")\
    .withColumnRenamed("lon","msg_lon")\
    .withColumn('user_id', F.col('event.message_from'))\
    .withColumn('event_id', F.monotonically_increasing_id())
```
С учетом добавленных полей, данные имеют следующую структуру:

![schema](https://user-images.githubusercontent.com/63814959/226731775-9290acd7-13ad-4c16-a851-ae6260547961.png)

Напишем функцию для определения реального города для каждого события. В функции воспользуемся описанной выше формулой расстояния между двумя тчоками:

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



Далее найдем актуальный адрес, то есть город в котором находился пользователь во время отправки последнего сообщения:

```python
window_act_city = Window().partitionBy('user_id').orderBy(F.col("date").desc())
act_city = events \
            .withColumn("row_number", F.row_number().over(window_act_city)) \
            .filter(F.col("row_number")==1) \
            .withColumnRenamed('city', 'act_city')
```

Составим список непрерывного пребывания в одном городе опираясь на дату сообщения из следующего. 
На основании этой выборки найдем список посещенных городов, их количество и домашний город:

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

Напишем функцию для определения локального времени актуального города:

```python
def calc_local_tm(events):    
    return events.withColumn("TIME",F.col("event.datetime").cast("Timestamp"))\
        .withColumn("local_time",F.from_utc_timestamp(F.col("TIME"),F.col('timezone')))\
        .select("local_time", 'user_id')
		
local_time = calc_local_tm(act_city)
```

Все необходимые данные готовы, осталось собрать итоговый результат:

```python
result = events \
        .join(act_city, ['user_id'], 'left') \
        .join(travels_array,['user_id'], 'left') \
        .join(home, ['user_id'], 'left') \
        .join(local_time, ['user_id'], 'left') \
        .selectExpr('user_id', 'act_city', 'home_city', 'travel_count', 'travel_array', 'local_time') \
        .distinct()
```
Итог выглядит так:


![image](https://user-images.githubusercontent.com/63814959/226732751-ca4aa9a4-85fc-47fd-ba32-e2b1c20059b5.png)


Для сборки финальной джобы потребуются 2 функции. 
Первая будет подтягивать спарк-сессию, вторая будет записывать итоговый результат.

Функция для инициализации спарк-сессии (в общем виде, т.к. не известны параметры кластера):

```python
def spark_session_init(name):
    return SparkSession \
        .builder \
        .master("yarn")\
        .appName(f"{name}") \
        .getOrCreate()
```

Функция для записи:

```python 
def write_df(df, df_name, date):
    df.write.mode('overwrite').parquet(f'/user/konstantin/prod/{df_name}/date={date}')
```

Функции вынесем в отдельный файл: **/src/scripts/tools.py**

Финальная джоба: **/src/scripts/dm_users.py** 

Локальный тестовый **.ipynb**: **/src/dm_users.ipynb** 

### 2.2 Витрина в разрезе зон

Создадим витрине с распределением различных событий по городам.
Данная витрина поможет понимать поведение пользователей в зависимости от географической зоны.

Для начала необходимо вновь прочитать данные. 
Этот шаг никак не будет отличаться от предыдущей витрины за исключением того, 
что отбирать будем все события, а не только с типом "сообщение".

В данных не предусмотрены события с типом "регистрация", поэтому для начала соберем без них:

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

Далее рассчитаем дату первого события для каждого пользователя и сджойним:

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
Джоба: **/src/scripts/dm_zone.py**

Локальный тестовый **.ipynb**: **/src/dm_zone.ipynb** 

### 2.3 Витрина рекомендаций

В финальной витрине необходимо собрать парные атрибуты (два айдишника) пользователей, 
которые могут быть порекомендованы друг другу.

Для начала прочитаем данные (как и при сборке предыдущих витрин).

После найдем координаты последнего отправленного сообщения по каждому пользователю:

```python 
window_last_msg = Window.partitionBy('user_id').orderBy(F.col('event.message_ts').desc())
last_msg = events.where("event_type == 'message'") \
    .where('msg_lon is not null') \
    .withColumn("rn",F.row_number().over(window_last)) \
    .filter(F.col('rn') == 1) \
    .drop(F.col('rn')) \
    .selectExpr('user_id', 'msg_lon as lon', 'msg_lat as lat', 'city', 'event.datetime as datetime', 'timezone')
```

Соберем список пользователей и каналов, на которые они подписаны с помощью self-join:

```python 
user_channel = events_geo.select(
    F.col('event.subscription_channel').alias('channel'),
    F.col('event.user').alias('user_id')
).distinct()

user_channel_f = user_channel \
            .join(user_channel.withColumnRenamed('user_id', 'user_id_2'), ['channel'], 'inner') \
            .filter('user_id < user_id_2')
```

Сджойним результат с сообщениями, чтобы получить актуальные координаты для каждого в паре пользователей:

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

По уже знакомой формуле найдем расстояние между пользователями и отфильтруем по <= 1 км:

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

Соберем список пользователей, которые писали друг другу:

```python 
events_pairs = events.selectExpr('event.message_from as user_id','event.message_to as user_id_2') \
               .where(F.col('user_id_2').isNotNull())
			   
events_pairs_union = events_pairs.union(events_pairs.select('user_id_2', 'user_id')).distinct()
```

Для получения итогового результата воспользуемся типом джойна **left-anti**, 
чтобы отсечь пользователей, которые уже переписывались:

```python 
result = distance \
    .join(events_pairs_union, ['user_id', 'user_id_2'], 'left_anti') \
    .withColumn('processed_dttm', F.current_timestamp()) \
    .selectExpr('user_id as user_left', 'user_id_2 as user_right', 'processed_dttm', 'city_1 as city', 'local_time') \
	.distinct()
```

Джоба: **/src/scripts/dm_rec.py**

Локальный тестовый **.ipynb**: **/src/dm_rec.ipynb** 

### 2.4 Автоматизация

Для того, чтобы витрины рассчитывались ежедневно соберем DAG и поставим его на регламент в Airflow.

Код DAG'a: **/src/dags/dag_social_rec.py**

## Итоги

В рамках данного проекта был разработан **Data Lake**, который содержит 4 слоя: RAW, ODS, Sandbox и DataMart.

Также были написаны на **pyspark** 3 джобы для сборки 3-х витрин.
После был написан DAG для регулярного обновления витрин.

Джобы лежат здесь: **/src/scripts**

Итоговый DAG: **/src/dags**

Для ознакомления загружены тестовые **Jupyter**-ноутбуки: **/src**
