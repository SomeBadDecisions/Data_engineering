# Microservice architecture

- 1 [Project description](#Project_description)
- 2 [Microservice development](#Microservice_development)
	- 2.1 [DDS](#DDS)
		- 2.1.1 [Docker-image](#Docker-image)
		- 2.1.2 [Docker-compose](#Docker-compose)
		- 2.1.3 [Helm-chart](#Helm-chart)
		- 2.1.4 [DDS-service](#DDS-service)
			- 2.1.4.1 [Connections](#Connections)
			- 2.1.4.2 [Postgres](#Postgres)
			- 2.1.4.3 [Kafka](#Kafka)
			- 2.1.4.4 [App logic](#App_logic)
	- 2.2 [CDM](#CDM)
		- 2.2.1 [Postgres](#Postgres_2)
		- 2.2.2 [Работа с Kafka](#Kafka_2)
- 3 [Conclusion](#Conclusion)

<a id="Project_description"></a>
## 1 Project description 

Within this project, it is necessary to develop a data storage for a food delivery aggregator. 
The company does not have its own resources, so to solve the task, it is necessary to use a cloud service. In this case, **Yandex.Cloud** was chosen. 

With it's help were deployed:
- **PostgreSQL**
- **Redis**
- **Kafka**
- **Container Registry** for docker images

DWH should contain 3 layers: 

- **STG** — raw data as is.
- **CDM** — two datamarts. First one — order counter for dishes; Second one — order counter by product categories.
- **DDS** — Data Vault data model.

DDS-layer scheme: 

![dds](https://github.com/SomeBadDecisions/Data_engineering/assets/63814959/1411f0db-ae32-4c7d-ade2-201aea6a9494)

Data from the source system is transmitted through two channels:

- The flow of orders that goes into the **Kafka** (5 per minute).
- Dictionary data (dishes, restaurants, users), that goes into **Redis**.

Raw data format: **JSON**.
Message broker: **Kafka**.
Database: **PostgreSQL**, **Redis**.

To implement the project, it is necessary to write 3 microservices (one for each layer) and deploy them in **Kubernetes**.

Business analysts will build BI reports in Data Lens based on the data from CDM.

The final pipeline will look as follows:

![app_scheme](https://github.com/SomeBadDecisions/Data_engineering/assets/63814959/a7041d6c-e11d-4853-b9da-7b023819633a)


I have already developed the first microservice for the STG layer: /cloud_service/**service_stg**.

<a id="Microservice_development"></a>
## 2 Microservice development 

<a id="DDS"></a>
### 2.1 DDS 

<a id="Docker-image"></a>
#### 2.1.1 Docker-image 

To begin with, let's create a docker image for the future service.

For the DDS service, we will need connections to **Kafka** and **PostgreSQL**.

For security purposes, we will not include the connection parameters in the code of the service itself, but rather set them through environment variables.

We will specify all the necessary libraries in cloud_service/service_dds/**requirements.txt** and install them from there in the future.

<details>
	<summary> **Docker-image:** </summary>

```python
	FROM python:3.10

	ARG KAFKA_HOST
	ARG KAFKA_PORT
	ARG KAFKA_CONSUMER_USERNAME
	ARG KAFKA_CONSUMER_PASSWORD
	ARG KAFKA_CONSUMER_GROUP
	ARG KAFKA_SOURCE_TOPIC
	ARG KAFKA_DESTINATION_TOPIC

	ARG PG_WAREHOUSE_HOST
	ARG PG_WAREHOUSE_PORT
	ARG PG_WAREHOUSE_DBNAME
	ARG PG_WAREHOUSE_USER
	ARG PG_WAREHOUSE_PASSWORD

	RUN apt-get update -y

	COPY . .

	RUN pip install -r requirements.txt

	RUN mkdir -p /crt
	RUN wget "https://storage.yandexcloud.net/cloud-certs/CA.pem" --output-document /crt/YandexInternalRootCA.crt
	RUN chmod 0600 /crt/YandexInternalRootCA.crt

	WORKDIR /src
	ENTRYPOINT ["python"]

	CMD ["app.py"]
```
</details>

<a id="Docker-compose"></a>
#### 2.1.2 Docker-compose 

Let's add a description of the DDS service to cloud_service/**docker-compose.yaml**:

```python
dds_service:
    build:
      context: ./service_dds
      network: host
    image: dds_service:local
    container_name: dds_service_container
    environment:
      FLASK_APP: ${DDS_APP:-dds_service}
      DEBUG: ${DDS_DEBUG:-True}

      KAFKA_HOST: ${KAFKA_HOST}
      KAFKA_PORT: ${KAFKA_PORT}
      KAFKA_CONSUMER_USERNAME: ${KAFKA_CONSUMER_USERNAME}
      KAFKA_CONSUMER_PASSWORD: ${KAFKA_CONSUMER_PASSWORD}
      KAFKA_CONSUMER_GROUP: ${KAFKA_CONSUMER_GROUP}
      KAFKA_SOURCE_TOPIC: ${KAFKA_STG_SERVICE_ORDERS_TOPIC}
      KAFKA_DESTINATION_TOPIC: ${KAFKA_DDS_SERVICE_ORDERS_TOPIC}

      PG_WAREHOUSE_HOST: ${PG_WAREHOUSE_HOST}
      PG_WAREHOUSE_PORT: ${PG_WAREHOUSE_PORT}
      PG_WAREHOUSE_DBNAME: ${PG_WAREHOUSE_DBNAME}
      PG_WAREHOUSE_USER: ${PG_WAREHOUSE_USER}
      PG_WAREHOUSE_PASSWORD: ${PG_WAREHOUSE_PASSWORD}

    network_mode: "bridge"
    ports:
      - "5012:5000"
    restart: unless-stopped
```
<a id="Helm-chart"></a>
#### 2.1.3 Helm-chart 

Let's prepare the files for release through Helm.

In cloud_service/service_dds/app/**Chart.yaml** we will specify the name of the service:

```python
apiVersion: v2
name: dds-service
description: A Helm chart for Kubernetes
type: application
version: 0.1.0
appVersion: "1.16.0"
```
Let's create cloud_service/service_dds/app/**values.yaml** (connection parameters here and in the code have been changed for security purposes):

```python
replicaCount: 1

image:
  repository: some_rep
  pullPolicy: IfNotPresent
  tag: "v2023-07-31-r1"

containerPort: 5000

config:
  KAFKA_HOST: "some_kafka_host"
  KAFKA_PORT: "some_kafka_port"
  KAFKA_CONSUMER_USERNAME: "producer_consumer"
  KAFKA_CONSUMER_PASSWORD: "pass"
  KAFKA_CONSUMER_GROUP: "main-consumer-group"
  KAFKA_SOURCE_TOPIC: "stg-service-orders"
  KAFKA_DESTINATION_TOPIC: "dds-service-orders"

  PG_WAREHOUSE_HOST: "pg_host"
  PG_WAREHOUSE_PORT: "port"
  PG_WAREHOUSE_DBNAME: "some_db"
  PG_WAREHOUSE_USER: "konstantin"
  PG_WAREHOUSE_PASSWORD: "pass"



imagePullSecrets: []
nameOverride: ""
fullnameOverride: ""

podAnnotations: {}

resources:
  limits:
    cpu: 100m
    memory: 128Mi
  requests:
    cpu: 100m
    memory: 128Mi
```

<a id="DDS-service"></a>
#### 2.1.4 DDS-service 

<a id="Connections"></a>
##### 2.1.4.1 Connections 

To begin with, let's create all the necessary connections. For the DDS layer, these are postgres and kafka.

Let's define the logic for connecting to kafka in cloud_service/service_dds/src/lib/kafka_connect/**kafka_connectors.py**:

```python
import json
from typing import Dict, Optional

from confluent_kafka import Consumer, Producer


def error_callback(err):
    print('Something went wrong: {}'.format(err))


class KafkaProducer:
    def __init__(self, host: str, port: int, user: str, password: str, topic: str, cert_path: str) -> None:
        params = {
            'bootstrap.servers': f'{host}:{port}',
            'security.protocol': 'SASL_SSL',
            'ssl.ca.location': cert_path,
            'sasl.mechanism': 'SCRAM-SHA-512',
            'sasl.username': user,
            'sasl.password': password,
            'error_cb': error_callback,
        }

        self.topic = topic
        self.p = Producer(params)

    def produce(self, payload: Dict) -> None:
        self.p.produce(self.topic, json.dumps(payload))
        self.p.flush(10)


class KafkaConsumer:
    def __init__(self,
                 host: str,
                 port: int,
                 user: str,
                 password: str,
                 topic: str,
                 group: str,
                 cert_path: str
                 ) -> None:
        params = {
            'bootstrap.servers': f'{host}:{port}',
            'security.protocol': 'SASL_SSL',
            'ssl.ca.location': cert_path,
            'sasl.mechanism': 'SCRAM-SHA-512',
            'sasl.username': user,
            'sasl.password': password,
            'group.id': group,  # '',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,
            'error_cb': error_callback,
            'debug': 'all',
            'client.id': 'someclientkey'
        }

        self.topic = topic
        self.c = Consumer(params)
        self.c.subscribe([topic])

    def consume(self, timeout: float = 3.0) -> Optional[Dict]:
        msg = self.c.poll(timeout=timeout)
        if not msg:
            return None
        if msg.error():
            raise Exception(msg.error())
        val = msg.value().decode()
        return json.loads(val)

```

Similarly, let's configure the connection to PostgreSQL in the file cloud_service/service_dds/src/lib/pg/**pg_connect.py**:

```python
from contextlib import contextmanager
from typing import Generator

import psycopg
from psycopg import Connection


class PgConnect:
    def __init__(self, host: str, port: int, db_name: str, user: str, pw: str, sslmode: str = "require") -> None:
        self.host = host
        self.port = port
        self.db_name = db_name
        self.user = user
        self.pw = pw
        self.sslmode = sslmode

    def url(self) -> str:
        return """
            host={host}
            port={port}
            dbname={db_name}
            user={user}
            password={pw}
            target_session_attrs=read-write
            sslmode={sslmode}
        """.format(
            host=self.host,
            port=self.port,
            db_name=self.db_name,
            user=self.user,
            pw=self.pw,
            sslmode=self.sslmode)

    @contextmanager
    def connection(self) -> Generator[Connection, None, None]:
        conn = psycopg.connect(self.url())
        try:
            yield conn
            conn.commit()
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            conn.close()

```

<a id="Postgres"></a>
##### 2.1.4.2 Postgres 
Next, we will write functions to fill the DDS layer tables in Postgres and save them to a file cloud_service/service_dds/src/dds_loader/repository/**dds_repository.py**:

```python
import uuid
from datetime import datetime
from typing import Any, Dict, List

from lib.pg import PgConnect
from pydantic import BaseModel


class H_User(BaseModel):
    h_user_pk: uuid.UUID
    user_id: str
    load_dt: datetime
    load_src: str


class H_Product(BaseModel):
    h_product_pk: uuid.UUID
    product_id: str
    load_dt: datetime
    load_src: str


class H_Category(BaseModel):
    h_category_pk: uuid.UUID
    category_name: str
    load_dt: datetime
    load_src: str


class H_Restaurant(BaseModel):
    h_restaurant_pk: uuid.UUID
    restaurant_id: str
    load_dt: datetime
    load_src: str


class H_Order(BaseModel):
    h_order_pk: uuid.UUID
    order_id: int
    order_dt: datetime
    load_dt: datetime
    load_src: str


class L_OrderProduct(BaseModel):
    hk_order_product_pk: uuid.UUID
    h_order_pk: uuid.UUID
    h_product_pk: uuid.UUID
    load_dt: datetime
    load_src: str


class L_ProductRestaurant(BaseModel):
    hk_product_restaurant_pk: uuid.UUID
    h_restaurant_pk: uuid.UUID
    h_product_pk: uuid.UUID
    load_dt: datetime
    load_src: str


class L_ProductCategory(BaseModel):
    hk_product_category_pk: uuid.UUID
    h_category_pk: uuid.UUID
    h_product_pk: uuid.UUID
    load_dt: datetime
    load_src: str


class L_OrderUser(BaseModel):
    hk_order_user_pk: uuid.UUID
    h_order_pk: uuid.UUID
    h_user_pk: uuid.UUID
    load_dt: datetime
    load_src: str


class S_UserNames(BaseModel):
    hk_user_names_pk: uuid.UUID
    h_user_pk: uuid.UUID
    username: str
    userlogin: str
    load_dt: datetime
    load_src: str


class S_RestaurantNames(BaseModel):
    hk_restaurant_names_pk: uuid.UUID
    h_restaurant_pk: uuid.UUID
    name: str
    load_dt: datetime
    load_src: str


class S_OrderCost(BaseModel):
    hk_order_cost_pk: uuid.UUID
    h_order_pk: uuid.UUID
    cost: float
    payment: float
    load_dt: datetime
    load_src: str


class S_OrderStatus(BaseModel):
    hk_order_status_pk: uuid.UUID
    h_order_pk: uuid.UUID
    status: str
    load_dt: datetime
    load_src: str


class S_ProductNames(BaseModel):
    hk_product_names_pk: uuid.UUID
    h_product_pk: uuid.UUID
    name: str
    load_dt: datetime
    load_src: str


class OrderDdsBuilder:
    def __init__(self, dict: Dict) -> None:
        self._dict = dict
        self.source_system = "orders-system-kafka"
        self.order_ns_uuid = uuid.UUID('7f288a2e-0ad0-4039-8e59-6c9838d87307')

    def _uuid(self, obj: Any) -> uuid.UUID:
        return uuid.uuid5(namespace=self.order_ns_uuid, name=str(obj))

    def h_user(self) -> H_User:
        user_id = self._dict['user']['id']
        return H_User(
            h_user_pk=self._uuid(user_id),
            user_id=user_id,
            load_dt=datetime.utcnow(),
            load_src=self.source_system
        )

    def h_product(self) -> List[H_Product]:
        products = []

        for prod_dict in self._dict['products']:
            prod_id = prod_dict['id']
            products.append(
                H_Product(
                    h_product_pk=self._uuid(prod_id),
                    product_id=prod_id,
                    load_dt=datetime.utcnow(),
                    load_src=self.source_system
                )
            )

        return products

    def h_category(self) -> List[H_Category]:
        categories = []

        for prod_dict in self._dict['products']:
            cat_name = prod_dict['category']
            categories.append(
                H_Category(
                    h_category_pk=self._uuid(cat_name),
                    category_name=cat_name,
                    load_dt=datetime.utcnow(),
                    load_src=self.source_system
                )
            )

        return categories

    def h_restaurant(self) -> H_Restaurant:
        restaurant_id = self._dict['restaurant']['id']
        return H_Restaurant(
            h_restaurant_pk=self._uuid(restaurant_id),
            restaurant_id=restaurant_id,
            load_dt=datetime.utcnow(),
            load_src=self.source_system
        )

    def h_order(self) -> H_Order:
        order_id = self._dict['id']
        return H_Order(
            h_order_pk=self._uuid(order_id),
            order_id=order_id,
            order_dt=datetime.strptime(self._dict['date'], "%Y-%m-%d %H:%M:%S"),
            load_dt=datetime.utcnow(),
            load_src=self.source_system
        )

    def l_order_product(self) -> List[L_OrderProduct]:
        product_links = []

        order_id = self._dict['id']
        for prod_dict in self._dict['products']:
            prod_id = prod_dict['id']
            product_links.append(
                L_OrderProduct(
                    hk_order_product_pk=self._uuid(f"{order_id}#$#{prod_id}"),
                    h_order_pk=self._uuid(order_id),
                    h_product_pk=self._uuid(prod_id),
                    load_dt=datetime.utcnow(),
                    load_src=self.source_system
                )
            )

        return product_links

    def l_product_restaurant(self) -> List[L_ProductRestaurant]:
        links = []

        restaurant_id = self._dict['restaurant']['id']
        for prod_dict in self._dict['products']:
            prod_id = prod_dict['id']
            links.append(
                L_ProductRestaurant(
                    hk_product_restaurant_pk=self._uuid(f"{prod_id}#$#{restaurant_id}"),
                    h_restaurant_pk=self._uuid(restaurant_id),
                    h_product_pk=self._uuid(prod_id),
                    load_dt=datetime.utcnow(),
                    load_src=self.source_system
                )
            )

        return links

    def l_product_category(self) -> List[L_ProductCategory]:
        links = []

        for prod_dict in self._dict['products']:
            prod_id = prod_dict['id']
            cat_name = prod_dict['category']
            links.append(
                L_ProductCategory(
                    hk_product_category_pk=self._uuid(f"{prod_id}#$#{cat_name}"),
                    h_category_pk=self._uuid(cat_name),
                    h_product_pk=self._uuid(prod_id),
                    load_dt=datetime.utcnow(),
                    load_src=self.source_system
                )
            )

        return links

    def l_order_user(self) -> L_OrderUser:
        order_id = self._dict['id']
        user_id = self._dict['user']['id']
        return L_OrderUser(
            hk_order_user_pk=self._uuid(f"{order_id}#$#{user_id}"),
            h_order_pk=self._uuid(order_id),
            h_user_pk=self._uuid(user_id),
            load_dt=datetime.utcnow(),
            load_src=self.source_system
        )

    def s_order_cost(self) -> S_OrderCost:
        order_id = self._dict['id']
        cost = self._dict['cost']
        payment = self._dict['payment']
        return S_OrderCost(
            hk_order_cost_pk=self._uuid(f"{order_id}#$#{cost}#$#{payment}"),
            h_order_pk=self._uuid(order_id),
            cost=cost,
            payment=payment,
            load_dt=datetime.utcnow(),
            load_src=self.source_system
        )

    def s_order_status(self) -> S_OrderStatus:
        order_id = self._dict['id']
        status = self._dict['status']
        return S_OrderStatus(
            hk_order_status_pk=self._uuid(f"{order_id}#$#{status}"),
            h_order_pk=self._uuid(order_id),
            status=status,
            load_dt=datetime.utcnow(),
            load_src=self.source_system
        )

    def s_restaurant_names(self) -> S_RestaurantNames:
        restaurant_id = self._dict['restaurant']['id']
        restaurant_name = self._dict['restaurant']['name']

        return S_RestaurantNames(
            hk_restaurant_names_pk=self._uuid(f"{restaurant_id}#$#{restaurant_name}"),
            h_restaurant_pk=self._uuid(restaurant_id),
            name=restaurant_name,
            load_dt=datetime.utcnow(),
            load_src=self.source_system
        )

    def s_user_names(self) -> S_UserNames:
        user_id = self._dict['user']['id']
        username = self._dict['user']['name']
        userlogin = self._dict['user']['name']

        return S_UserNames(
            hk_user_names_pk=self._uuid(f"{user_id}#$#{username}#$#{userlogin}"),
            h_user_pk=self._uuid(user_id),
            username=username,
            userlogin=userlogin,
            load_dt=datetime.utcnow(),
            load_src=self.source_system
        )

    def s_product_names(self) -> List[S_ProductNames]:
        prod_names = []

        for prod_dict in self._dict['products']:
            prod_id = prod_dict['id']
            name = prod_dict['name']
            prod_names.append(
                S_ProductNames(
                    hk_product_names_pk=self._uuid(f"{prod_id}#$#{name}"),
                    h_product_pk=self._uuid(prod_id),
                    name=name,
                    load_dt=datetime.utcnow(),
                    load_src=self.source_system
                )
            )

        return prod_names


class DdsRepository:
    def __init__(self, db: PgConnect) -> None:
        self._db = db

    def h_user_insert(self, user: H_User) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.h_user(
                            h_user_pk,
                            user_id,
                            load_dt,
                            load_src
                        )
                        VALUES(
                            %(h_user_pk)s,
                            %(user_id)s,
                            %(load_dt)s,
                            %(load_src)s
                        )
                        ON CONFLICT (h_user_pk) DO NOTHING;
                    """,
                    {
                        'h_user_pk': user.h_user_pk,
                        'user_id': user.user_id,
                        'load_dt': user.load_dt,
                        'load_src': user.load_src
                    }
                )

    def h_product_insert(self, obj: H_Product) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.h_product(
                            h_product_pk,
                            product_id,
                            load_dt,
                            load_src
                        )
                        VALUES(
                            %(h_product_pk)s,
                            %(product_id)s,
                            %(load_dt)s,
                            %(load_src)s
                        )
                        ON CONFLICT (h_product_pk) DO NOTHING;
                    """,
                    {
                        'h_product_pk': obj.h_product_pk,
                        'product_id': obj.product_id,
                        'load_dt': obj.load_dt,
                        'load_src': obj.load_src
                    }
                )

    def h_category_insert(self, obj: H_Category) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.h_category(
                            h_category_pk,
                            category_name,
                            load_dt,
                            load_src
                        )
                        VALUES(
                            %(h_category_pk)s,
                            %(category_name)s,
                            %(load_dt)s,
                            %(load_src)s
                        )
                        ON CONFLICT (h_category_pk) DO NOTHING;
                    """,
                    {
                        'h_category_pk': obj.h_category_pk,
                        'category_name': obj.category_name,
                        'load_dt': obj.load_dt,
                        'load_src': obj.load_src
                    }
                )

    def h_restaurant_insert(self, obj: H_Restaurant) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.h_restaurant(
                            h_restaurant_pk,
                            restaurant_id,
                            load_dt,
                            load_src
                        )
                        VALUES(
                            %(h_restaurant_pk)s,
                            %(restaurant_id)s,
                            %(load_dt)s,
                            %(load_src)s
                        )
                        ON CONFLICT (h_restaurant_pk) DO NOTHING;
                    """,
                    {
                        'h_restaurant_pk': obj.h_restaurant_pk,
                        'restaurant_id': obj.restaurant_id,
                        'load_dt': obj.load_dt,
                        'load_src': obj.load_src
                    }
                )

    def h_order_insert(self, obj: H_Order) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.h_order(
                            h_order_pk,
                            order_id,
                            order_dt,
                            load_dt,
                            load_src
                        )
                        VALUES(
                            %(h_order_pk)s,
                            %(order_id)s,
                            %(order_dt)s,
                            %(load_dt)s,
                            %(load_src)s
                        )
                        ON CONFLICT (h_order_pk) DO NOTHING;
                    """,
                    {
                        'h_order_pk': obj.h_order_pk,
                        'order_id': obj.order_id,
                        'order_dt': obj.order_dt,
                        'load_dt': obj.load_dt,
                        'load_src': obj.load_src
                    }
                )

    def l_order_product_insert(self, obj: L_OrderProduct) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.l_order_product(
                            hk_order_product_pk,
                            h_order_pk,
                            h_product_pk,
                            load_dt,
                            load_src
                        )
                        VALUES(
                            %(hk_order_product_pk)s,
                            %(h_order_pk)s,
                            %(h_product_pk)s,
                            %(load_dt)s,
                            %(load_src)s
                        )
                        ON CONFLICT (hk_order_product_pk) DO NOTHING;
                    """,
                    {
                        'hk_order_product_pk': obj.hk_order_product_pk,
                        'h_order_pk': obj.h_order_pk,
                        'h_product_pk': obj.h_product_pk,
                        'load_dt': obj.load_dt,
                        'load_src': obj.load_src
                    }
                )

    def l_product_restaurant_insert(self, obj: L_ProductRestaurant) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.l_product_restaurant(
                            hk_product_restaurant_pk,
                            h_restaurant_pk,
                            h_product_pk,
                            load_dt,
                            load_src
                        )
                        VALUES(
                            %(hk_product_restaurant_pk)s,
                            %(h_restaurant_pk)s,
                            %(h_product_pk)s,
                            %(load_dt)s,
                            %(load_src)s
                        )
                        ON CONFLICT (hk_product_restaurant_pk) DO NOTHING;
                    """,
                    {
                        'hk_product_restaurant_pk': obj.hk_product_restaurant_pk,
                        'h_restaurant_pk': obj.h_restaurant_pk,
                        'h_product_pk': obj.h_product_pk,
                        'load_dt': obj.load_dt,
                        'load_src': obj.load_src
                    }
                )

    def l_product_category_insert(self, obj: L_ProductCategory) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.l_product_category(
                            hk_product_category_pk,
                            h_category_pk,
                            h_product_pk,
                            load_dt,
                            load_src
                        )
                        VALUES(
                            %(hk_product_category_pk)s,
                            %(h_category_pk)s,
                            %(h_product_pk)s,
                            %(load_dt)s,
                            %(load_src)s
                        )
                        ON CONFLICT (hk_product_category_pk) DO NOTHING;
                    """,
                    {
                        'hk_product_category_pk': obj.hk_product_category_pk,
                        'h_category_pk': obj.h_category_pk,
                        'h_product_pk': obj.h_product_pk,
                        'load_dt': obj.load_dt,
                        'load_src': obj.load_src
                    }
                )

    def l_order_user_insert(self, obj: L_OrderUser) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.l_order_user(
                            hk_order_user_pk,
                            h_order_pk,
                            h_user_pk,
                            load_dt,
                            load_src
                        )
                        VALUES(
                            %(hk_order_user_pk)s,
                            %(h_order_pk)s,
                            %(h_user_pk)s,
                            %(load_dt)s,
                            %(load_src)s
                        )
                        ON CONFLICT (hk_order_user_pk) DO NOTHING;
                    """,
                    {
                        'hk_order_user_pk': obj.hk_order_user_pk,
                        'h_order_pk': obj.h_order_pk,
                        'h_user_pk': obj.h_user_pk,
                        'load_dt': obj.load_dt,
                        'load_src': obj.load_src
                    }
                )

    def s_user_names_insert(self, obj: S_UserNames) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.s_user_names(
                            hk_user_names_pk,
                            h_user_pk,
                            username,
                            userlogin,
                            load_dt,
                            load_src
                        )
                        VALUES(
                            %(hk_user_names_pk)s,
                            %(h_user_pk)s,
                            %(username)s,
                            %(userlogin)s,
                            %(load_dt)s,
                            %(load_src)s
                        )
                        ON CONFLICT (hk_user_names_pk) DO NOTHING;
                    """,
                    {
                        'hk_user_names_pk': obj.hk_user_names_pk,
                        'h_user_pk': obj.h_user_pk,
                        'username': obj.username,
                        'userlogin': obj.userlogin,
                        'load_dt': obj.load_dt,
                        'load_src': obj.load_src
                    }
                )

    def s_restaurant_names_insert(self, obj: S_RestaurantNames) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.s_restaurant_names(
                            hk_restaurant_names_pk,
                            h_restaurant_pk,
                            name,
                            load_dt,
                            load_src
                        )
                        VALUES(
                            %(hk_restaurant_names_pk)s,
                            %(h_restaurant_pk)s,
                            %(name)s,
                            %(load_dt)s,
                            %(load_src)s
                        )
                        ON CONFLICT (hk_restaurant_names_pk) DO NOTHING;
                    """,
                    {
                        'hk_restaurant_names_pk': obj.hk_restaurant_names_pk,
                        'h_restaurant_pk': obj.h_restaurant_pk,
                        'name': obj.name,
                        'load_dt': obj.load_dt,
                        'load_src': obj.load_src
                    }
                )

    def s_product_names_insert(self, obj: S_ProductNames) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.s_product_names(
                            hk_product_names_pk,
                            h_product_pk,
                            name,
                            load_dt,
                            load_src
                        )
                        VALUES(
                            %(hk_product_names_pk)s,
                            %(h_product_pk)s,
                            %(name)s,
                            %(load_dt)s,
                            %(load_src)s
                        )
                        ON CONFLICT (hk_product_names_pk) DO NOTHING;
                    """,
                    {
                        'hk_product_names_pk': obj.hk_product_names_pk,
                        'h_product_pk': obj.h_product_pk,
                        'name': obj.name,
                        'load_dt': obj.load_dt,
                        'load_src': obj.load_src
                    }
                )

    def s_order_cost_insert(self, obj: S_OrderCost) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.s_order_cost(
                            hk_order_cost_pk,
                            h_order_pk,
                            cost,
                            payment,
                            load_dt,
                            load_src
                        )
                        VALUES(
                            %(hk_order_cost_pk)s,
                            %(h_order_pk)s,
                            %(cost)s,
                            %(payment)s,
                            %(load_dt)s,
                            %(load_src)s
                        )
                        ON CONFLICT (hk_order_cost_pk) DO NOTHING;
                    """,
                    {
                        'hk_order_cost_pk': obj.hk_order_cost_pk,
                        'h_order_pk': obj.h_order_pk,
                        'cost': obj.cost,
                        'payment': obj.payment,
                        'load_dt': obj.load_dt,
                        'load_src': obj.load_src
                    }
                )

    def s_order_status_insert(self, obj: S_OrderStatus) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.s_order_status(
                            hk_order_status_pk,
                            h_order_pk,
                            status,
                            load_dt,
                            load_src
                        )
                        VALUES(
                            %(hk_order_status_pk)s,
                            %(h_order_pk)s,
                            %(status)s,
                            %(load_dt)s,
                            %(load_src)s
                        )
                        ON CONFLICT (hk_order_status_pk) DO NOTHING;
                    """,
                    {
                        'hk_order_status_pk': obj.hk_order_status_pk,
                        'h_order_pk': obj.h_order_pk,
                        'status': obj.status,
                        'load_dt': obj.load_dt,
                        'load_src': obj.load_src
                    }
                )

```
In order to ensure idempotence, we will write additional functions to check the presence of all necessary tables in Postgres and create them if they are missing. 
All necessary functions containing DDL will be placed in the file cloud_service/service_dds/src/dds_loader/repository/**dds_migrations.py**:

```python
from lib.pg import PgConnect
from psycopg import Cursor


class DdsMigrator:
    def __init__(self, db: PgConnect) -> None:
        self._db = db

    def up(self) -> None:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                self._schema(cur)
                self._h_user_create(cur)
                self._h_product_create(cur)
                self._h_category_create(cur)
                self._h_restaurant_create(cur)
                self._h_order_create(cur)
                self._l_order_product_create(cur)
                self._l_product_restaurant_create(cur)
                self._l_product_category_create(cur)
                self._l_order_user_create(cur)
                self._s_user_names(cur)
                self._s_restaurant_names(cur)
                self._s_product_names(cur)
                self._s_order_cost(cur)
                self._s_order_status(cur)

    def _schema(self, cursor: Cursor) -> None:
        cursor.execute(
            """
                CREATE SCHEMA IF NOT EXISTS dds;
            """
        )

    def _h_user_create(self, cursor: Cursor) -> None:
        cursor.execute(
            """
                CREATE TABLE IF NOT EXISTS dds.h_user(
                    h_user_pk UUID NOT NULL PRIMARY KEY,
                    user_id VARCHAR NOT NULL,
                    load_dt timestamp NOT NULL,
                    load_src VARCHAR NOT NULL
                );
            """
        )

    def _h_product_create(self, cursor: Cursor) -> None:
        cursor.execute(
            """
                CREATE TABLE IF NOT EXISTS dds.h_product(
                    h_product_pk UUID NOT NULL PRIMARY KEY,
                    product_id VARCHAR NOT NULL,
                    load_dt timestamp NOT NULL,
                    load_src VARCHAR NOT NULL
                );
            """
        )

    def _h_category_create(self, cursor: Cursor) -> None:
        cursor.execute(
            """
                CREATE TABLE IF NOT EXISTS dds.h_category(
                    h_category_pk UUID NOT NULL PRIMARY KEY,
                    category_name VARCHAR NOT NULL,
                    load_dt timestamp NOT NULL,
                    load_src VARCHAR NOT NULL
                );
            """
        )

    def _h_restaurant_create(self, cursor: Cursor) -> None:
        cursor.execute(
            """
                CREATE TABLE IF NOT EXISTS dds.h_restaurant(
                    h_restaurant_pk UUID NOT NULL PRIMARY KEY,
                    restaurant_id VARCHAR NOT NULL,
                    load_dt timestamp NOT NULL,
                    load_src VARCHAR NOT NULL
                );
            """
        )

    def _h_order_create(self, cursor: Cursor) -> None:
        cursor.execute(
            """
                CREATE TABLE IF NOT EXISTS dds.h_order(
                    h_order_pk UUID NOT NULL PRIMARY KEY,
                    order_id INT NOT NULL,
                    order_dt timestamp NOT NULL,
                    load_dt timestamp NOT NULL,
                    load_src VARCHAR NOT NULL
                );
            """
        )

    def _l_order_product_create(self, cursor: Cursor) -> None:
        cursor.execute(
            """
                CREATE TABLE IF NOT EXISTS dds.l_order_product(
                    hk_order_product_pk UUID NOT NULL PRIMARY KEY,
                    h_order_pk UUID NOT NULL REFERENCES dds.h_order(h_order_pk),
                    h_product_pk UUID NOT NULL REFERENCES dds.h_product(h_product_pk),
                    load_dt timestamp NOT NULL,
                    load_src VARCHAR NOT NULL
                );
            """
        )

    def _l_product_restaurant_create(self, cursor: Cursor) -> None:
        cursor.execute(
            """
                CREATE TABLE IF NOT EXISTS dds.l_product_restaurant(
                    hk_product_restaurant_pk UUID NOT NULL PRIMARY KEY,
                    h_restaurant_pk UUID NOT NULL REFERENCES dds.h_restaurant(h_restaurant_pk),
                    h_product_pk UUID NOT NULL REFERENCES dds.h_product(h_product_pk),
                    load_dt timestamp NOT NULL,
                    load_src VARCHAR NOT NULL
                );
            """
        )

    def _l_product_category_create(self, cursor: Cursor) -> None:
        cursor.execute(
            """
                CREATE TABLE IF NOT EXISTS dds.l_product_category(
                    hk_product_category_pk UUID NOT NULL PRIMARY KEY,
                    h_category_pk UUID NOT NULL REFERENCES dds.h_category(h_category_pk),
                    h_product_pk UUID NOT NULL REFERENCES dds.h_product(h_product_pk),
                    load_dt timestamp NOT NULL,
                    load_src VARCHAR NOT NULL
                );
            """
        )

    def _l_order_user_create(self, cursor: Cursor) -> None:
        cursor.execute(
            """
                CREATE TABLE IF NOT EXISTS dds.l_order_user(
                    hk_order_user_pk UUID NOT NULL PRIMARY KEY,
                    h_order_pk UUID NOT NULL REFERENCES dds.h_order(h_order_pk),
                    h_user_pk UUID NOT NULL REFERENCES dds.h_user(h_user_pk),
                    load_dt timestamp NOT NULL,
                    load_src VARCHAR NOT NULL
                );
            """
        )

    def _s_user_names(self, cursor: Cursor) -> None:
        cursor.execute(
            """
                CREATE TABLE IF NOT EXISTS dds.s_user_names(
                    hk_user_names_pk UUID NOT NULL PRIMARY KEY,
                    h_user_pk UUID NOT NULL REFERENCES dds.h_user(h_user_pk),
                    username VARCHAR NOT NULL,
                    userlogin VARCHAR NOT NULL,
                    load_dt timestamp NOT NULL,
                    load_src VARCHAR NOT NULL
                );
            """
        )

    def _s_product_names(self, cursor: Cursor) -> None:
        cursor.execute(
            """
                CREATE TABLE IF NOT EXISTS dds.s_product_names(
                    hk_product_names_pk UUID NOT NULL PRIMARY KEY,
                    h_product_pk UUID NOT NULL REFERENCES dds.h_product(h_product_pk),
                    name VARCHAR NOT NULL,
                    load_dt timestamp NOT NULL,
                    load_src VARCHAR NOT NULL
                );
            """
        )

    def _s_restaurant_names(self, cursor: Cursor) -> None:
        cursor.execute(
            """
                CREATE TABLE IF NOT EXISTS dds.s_restaurant_names(
                    hk_restaurant_names_pk UUID NOT NULL PRIMARY KEY,
                    h_restaurant_pk UUID NOT NULL REFERENCES dds.h_restaurant(h_restaurant_pk),
                    name VARCHAR NOT NULL,
                    load_dt timestamp NOT NULL,
                    load_src VARCHAR NOT NULL
                );
            """
        )

    def _s_order_cost(self, cursor: Cursor) -> None:
        cursor.execute(
            """
                CREATE TABLE IF NOT EXISTS dds.s_order_cost(
                    hk_order_cost_pk UUID NOT NULL PRIMARY KEY,
                    h_order_pk UUID NOT NULL REFERENCES dds.h_order(h_order_pk),
                    cost decimal(19, 5) NOT NULL DEFAULT(0) CHECK(cost >= 0),
                    payment decimal(19, 5) NOT NULL DEFAULT(0) CHECK(payment >= 0),
                    load_dt timestamp NOT NULL,
                    load_src VARCHAR NOT NULL
                );
            """
        )

    def _s_order_status(self, cursor: Cursor) -> None:
        cursor.execute(
            """
                CREATE TABLE IF NOT EXISTS dds.s_order_status(
                    hk_order_status_pk UUID NOT NULL PRIMARY KEY,
                    h_order_pk UUID NOT NULL REFERENCES dds.h_order(h_order_pk),
                    status VARCHAR NOT NULL,
                    load_dt timestamp NOT NULL,
                    load_src VARCHAR NOT NULL
                );
            """
        )

```
<a id="Kafka"></a>
##### 2.1.4.3 Kafka 

Let's move on to data transmission in Kafka. We will describe the logic of receiving data and their subsequent transfer to a new topic in the file cloud_service/service_dds/src/dds_loader/**dds_message_processor_job.py**:

```python
from datetime import datetime
from logging import Logger
from typing import Dict, List

from lib.kafka_connect import KafkaConsumer, KafkaProducer

from dds_loader.repository.dds_repository import DdsRepository, OrderDdsBuilder


class DdsMessageProcessor:
    def __init__(self,
                 consumer: KafkaConsumer,
                 producer: KafkaProducer,
                 dds: DdsRepository,
                 logger: Logger) -> None:
        self._consumer = consumer
        self._producer = producer
        self._repository = dds

        self._logger = logger

        self._batch_size = 30

    def run(self) -> None:
        self._logger.info(f"{datetime.utcnow()}: START")

        for _ in range(self._batch_size):
            msg = self._consumer.consume()
            if not msg:
                self._logger.info(f"{datetime.utcnow()}: NO messages. Quitting.")
                break

            self._logger.info(f"{datetime.utcnow()}: {msg}")

            order_dict = msg['payload']
            builder = OrderDdsBuilder(order_dict)

            self._load_hubs(builder)
            self._load_links(builder)
            self._load_sats(builder)

            dst_msg = {
                "object_id": str(builder.h_order().h_order_pk),
                "sent_dttm": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                "object_type": "order_report",
                "payload": {
                    "id": str(builder.h_order().h_order_pk),
                    "order_dt": builder.h_order().order_dt.strftime("%Y-%m-%d %H:%M:%S"),
                    "status": builder.s_order_status().status,
                    "restaurant": {
                        "id": str(builder.h_restaurant().h_restaurant_pk),
                        "name": builder.s_restaurant_names().name
                    },
                    "user": {
                        "id": str(builder.h_user().h_user_pk),
                        "username": builder.s_user_names().username
                    },
                    "products": self._format_products(builder)
                }
            }

            self._logger.info(f"{datetime.utcnow()}: {dst_msg}")
            self._producer.produce(dst_msg)

        self._logger.info(f"{datetime.utcnow()}: FINISH")

    def _load_hubs(self, builder: OrderDdsBuilder) -> None:

        self._repository.h_user_insert(builder.h_user())

        for p in builder.h_product():
            self._repository.h_product_insert(p)

        for c in builder.h_category():
            self._repository.h_category_insert(c)

        self._repository.h_restaurant_insert(builder.h_restaurant())

        self._repository.h_order_insert(builder.h_order())

    def _load_links(self, builder: OrderDdsBuilder) -> None:
        self._repository.l_order_user_insert(builder.l_order_user())

        for op_link in builder.l_order_product():
            self._repository.l_order_product_insert(op_link)

        for pr_link in builder.l_product_restaurant():
            self._repository.l_product_restaurant_insert(pr_link)

        for pc_link in builder.l_product_category():
            self._repository.l_product_category_insert(pc_link)

    def _load_sats(self, builder: OrderDdsBuilder) -> None:
        self._repository.s_order_cost_insert(builder.s_order_cost())
        self._repository.s_order_status_insert(builder.s_order_status())
        self._repository.s_restaurant_names_insert(builder.s_restaurant_names())
        self._repository.s_user_names_insert(builder.s_user_names())

        for pn in builder.s_product_names():
            self._repository.s_product_names_insert(pn)

    def _format_products(self, builder: OrderDdsBuilder) -> List[Dict]:
        products = []

        p_names = {x.h_product_pk: x.name for x in builder.s_product_names()}

        cat_names = {x.h_category_pk: {"id": str(x.h_category_pk), "name": x.category_name} for x in builder.h_category()}
        prod_cats = {x.h_product_pk: cat_names[x.h_category_pk] for x in builder.l_product_category()}

        for p in builder.h_product():
            msg_prod = {
                "id": str(p.h_product_pk),
                "name": p_names[p.h_product_pk],
                "category": prod_cats[p.h_product_pk]
            }

            products.append(msg_prod)

        return products

```
<a id="App logic"></a>
##### 2.1.4.4 App logic 
The main logic of the service is described in cloud_service/service_dds/src/**app.py**.

To work with the service, we will use the class **BackgroundScheduler**, which will launch a worker at a specified interval.

In this case, the run function in the proc object is the entry point to the main logic of message processing.

With the specified interval, the scheduler will launch the run function, inside which messages are read and processed from Kafka.

Thus, micro-batch processing will be implemented.

```python
import logging

from apscheduler.schedulers.background import BackgroundScheduler
from flask import Flask

from app_config import AppConfig
from dds_loader.dds_message_processor_job import DdsMessageProcessor

app = Flask(__name__)

config = AppConfig()

if __name__ == '__main__':
    app.logger.setLevel(logging.DEBUG)

    migrator = DdsMigrator(config.pg_warehouse_db())
    migrator.up()

    proc = DdsMessageProcessor(
        config.kafka_consumer(),
        config.kafka_producer(),
        DdsRepository(config.pg_warehouse_db()),
        app.logger
    )

    scheduler = BackgroundScheduler()
    scheduler.add_job(func=proc.run, trigger="interval", seconds=25)
    scheduler.start()

    app.run(debug=True, host='0.0.0.0', use_reloader=False)

```

Let's create a file cloud_service/service_dds/src/**app_config.py**, which will contain the connection parameters to the consumer and producer in Kafka, and the database in Postgres:

```python
import os

from lib.kafka_connect import KafkaConsumer, KafkaProducer
from lib.pg import PgConnect


class AppConfig:
    CERTIFICATE_PATH = '/crt/YandexInternalRootCA.crt'

    def __init__(self) -> None:

        self.kafka_host = str(os.getenv('KAFKA_HOST'))
        self.kafka_port = int(str(os.getenv('KAFKA_PORT')))
        self.kafka_consumer_username = str(os.getenv('KAFKA_CONSUMER_USERNAME'))
        self.kafka_consumer_password = str(os.getenv('KAFKA_CONSUMER_PASSWORD'))
        self.kafka_consumer_group = str(os.getenv('KAFKA_CONSUMER_GROUP'))
        self.kafka_consumer_topic = str(os.getenv('KAFKA_SOURCE_TOPIC'))
        self.kafka_producer_username = str(os.getenv('KAFKA_CONSUMER_USERNAME'))
        self.kafka_producer_password = str(os.getenv('KAFKA_CONSUMER_PASSWORD'))
        self.kafka_producer_topic = str(os.getenv('KAFKA_DESTINATION_TOPIC'))

        self.pg_warehouse_host = str(os.getenv('PG_WAREHOUSE_HOST'))
        self.pg_warehouse_port = int(str(os.getenv('PG_WAREHOUSE_PORT')))
        self.pg_warehouse_dbname = str(os.getenv('PG_WAREHOUSE_DBNAME'))
        self.pg_warehouse_user = str(os.getenv('PG_WAREHOUSE_USER'))
        self.pg_warehouse_password = str(os.getenv('PG_WAREHOUSE_PASSWORD'))

    def kafka_producer(self):
        return KafkaProducer(
            self.kafka_host,
            self.kafka_port,
            self.kafka_producer_username,
            self.kafka_producer_password,
            self.kafka_producer_topic,
            self.CERTIFICATE_PATH
        )

    def kafka_consumer(self):
        return KafkaConsumer(
            self.kafka_host,
            self.kafka_port,
            self.kafka_consumer_username,
            self.kafka_consumer_password,
            self.kafka_consumer_topic,
            self.kafka_consumer_group,
            self.CERTIFICATE_PATH
        )

    def pg_warehouse_db(self):
        return PgConnect(
            self.pg_warehouse_host,
            self.pg_warehouse_port,
            self.pg_warehouse_dbname,
            self.pg_warehouse_user,
            self.pg_warehouse_password
        )
```

The development of the DDS service is complete.

<a id="CDM"></a>
### 2.2 CDM 

Most of the steps for creating a CDM service will be similar, so we will not go into detail about them. 
The difference will be in the logic of filling the tables in Postgres and receiving data from the Kafka topic.

<a id="Postgres_2"></a>
#### 2.2.1 Postgres 

By analogy, let's describe the DDL for the CDM layer in the file cloud_service/service_cdm/src/cdm_loader/repository/**cdm_migrations.py**.

The logic of filling the tables is located in the file cloud_service/service_cdm/src/cdm_loader/repository/**cdm_repository.py** and looks as follows:

```python
from uuid import UUID

from lib.pg import PgConnect


class UserProductCounterRepository:
    def __init__(self, db: PgConnect) -> None:
        self._db = db

    def inc(self, user: UUID, product_id: UUID, product_name: str) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO cdm.user_product_counters(
                            user_id,
                            product_id,
                            product_name,
                            order_cnt
                        )
                        VALUES(
                            %(user_id)s,
                            %(product_id)s,
                            %(product_name)s,
                            1
                        )
                        ON CONFLICT (user_id, product_id) DO UPDATE SET
                            order_cnt = user_product_counters.order_cnt + 1,
                            product_name = EXCLUDED.product_name
                        ;
                    """,
                    {
                        'user_id': user,
                        'product_id': product_id,
                        'product_name': product_name
                    }
                )


class UserCategoryCounterRepository:
    def __init__(self, db: PgConnect) -> None:
        self._db = db

    def inc(self, user: UUID, category_id: UUID, category_name: str) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO cdm.user_category_counters(
                            user_id,
                            category_id,
                            category_name,
                            order_cnt
                        )
                        VALUES(
                            %(user_id)s,
                            %(category_id)s,
                            %(category_name)s,
                            1
                        )
                        ON CONFLICT (user_id, category_id) DO UPDATE SET
                            order_cnt = user_category_counters.order_cnt + 1,
                            category_name = EXCLUDED.category_name
                        ;
                    """,
                    {
                        'user_id': user,
                        'category_id': category_id,
                        'category_name': category_name
                    }
                )


class RestaurantCategoryCounterRepository:
    def __init__(self, db: PgConnect) -> None:
        self._db = db

    def inc(self, restaurant_id: UUID, restaurant_name: str, category_id: UUID, category_name: str) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO cdm.restaurant_category_counters(
                            restaurant_id,
                            restaurant_name,
                            category_id,
                            category_name,
                            order_cnt
                        )
                        VALUES(
                            %(restaurant_id)s,
                            %(restaurant_name)s,
                            %(category_id)s,
                            %(category_name)s,
                            1
                        )
                        ON CONFLICT (restaurant_id, category_id) DO UPDATE SET
                            order_cnt = restaurant_category_counters.order_cnt + 1,
                            category_name = EXCLUDED.category_name,
                            restaurant_name = EXCLUDED.restaurant_name
                        ;
                    """,
                    {
                        'restaurant_id': restaurant_id,
                        'restaurant_name': restaurant_name,
                        'category_id': category_id,
                        'category_name': category_name
                    }
                )

```
<a id="Kafka_2"></a>
#### 2.2.2 Kafka 

The logic of message reception will be described in the file cloud_service/service_cdm/src/cdm_loader/**cdm_message_processor_job.py**:

```python
from datetime import datetime
from logging import Logger
from uuid import UUID

from lib.kafka_connect import KafkaConsumer

from cdm_loader.repository import (RestaurantCategoryCounterRepository,
                                   UserCategoryCounterRepository,
                                   UserProductCounterRepository)


class CdmMessageProcessor:
    def __init__(self,
                 consumer: KafkaConsumer,
                 userproductcnt: UserProductCounterRepository,
                 usercategorycnt: UserCategoryCounterRepository,
                 restcategorycnt: RestaurantCategoryCounterRepository,
                 logger: Logger,
                 ) -> None:
        self._consumer = consumer
        self._userproductcnt = userproductcnt
        self._usercategorycnt = usercategorycnt
        self._restcategorycnt = restcategorycnt

        self._logger = logger

        self._batch_size = 3

    def run(self) -> None:
        self._logger.info(f"{datetime.utcnow()}: START")

        for _ in range(self._batch_size):
            msg = self._consumer.consume()
            if not msg:
                break

            self._logger.info(f"{datetime.utcnow()}: {msg}")

            order = msg['payload']
            if order['status'] == 'CANCELLED':
                self._logger.info(f"{datetime.utcnow()}: CANCELLED. Skipping.")
                continue

            user_id = UUID(order['user']['id'])
            cat_dict = {}
            for p in order['products']:
                prod_id = UUID(p['id'])
                prod_name = p['name']
                self._userproductcnt.inc(user_id, prod_id, prod_name)

                cat_dict[p['category']['id']] = p['category']['name']

            rest_id = UUID(order['restaurant']['id'])
            rest_name = order['restaurant']['name']
            for (cat_id, cat_name) in cat_dict.items():
                self._restcategorycnt.inc(rest_id, rest_name, UUID(cat_id), cat_name)

        self._logger.info(f"{datetime.utcnow()}: FINISH")

```

As I mentioned earlier, the remaining steps, such as filling in docker and docker-compose files, preparing yaml for release in Helm, and creating connections, are practically identical.

This concludes the creation of the CDM service.

After the development of the services was completed, corresponding Docker images were created and pushed to the registry.
DDS and CDM services are fully ready for release in Helm and further autonomous operation.

<a id="Conclusion"></a>
## 3 Conclusion

Within this project:

- Kafka, Postgres, and Redis were deployed in the cloud

- A schema for the DDS layer was developed based on the Data Vault model

- A DDS service was written to read data from the Kafka topic, save it in the DDS layer of the Postgres database, and transfer it to a new Kafka topic

- A CDM service was written to retrieve data from the Kafka topic and save it in the CDM layer of the Postgres database
