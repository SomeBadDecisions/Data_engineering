import logging

import pendulum
from airflow.decorators import dag, task
from airflow.models.variable import Variable
from examples.stg.mongo_dag.restaurants_saver import RestaurantsSaver
from examples.stg.mongo_dag.restaurants_loader import RestaurantLoader
from examples.stg.mongo_dag.restaurant_reader import RestaurantReader
from examples.stg.mongo_dag.users_saver import UsersSaver
from examples.stg.mongo_dag.users_loader import UsersLoader
from examples.stg.mongo_dag.users_reader import UsersReader
from examples.stg.mongo_dag.orders_saver import OrdersSaver
from examples.stg.mongo_dag.orders_loader import OrdersLoader
from examples.stg.mongo_dag.orders_reader import OrdersReader
from lib import ConnectionBuilder, MongoConnect

log = logging.getLogger(__name__)


@dag(
    schedule_interval='0/15 * * * *',  # Задаем расписание выполнения дага - каждый 15 минут.
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['sprint5', 'example', 'stg', 'origin'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=True  # Остановлен/запущен при появлении. Сразу запущен.
)
def mongo_dag():
    # Создаем подключение к базе dwh.
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    # Получаем переменные из Airflow.
    cert_path = Variable.get("MONGO_DB_CERTIFICATE_PATH")
    db_user = Variable.get("MONGO_DB_USER")
    db_pw = Variable.get("MONGO_DB_PASSWORD")
    rs = Variable.get("MONGO_DB_REPLICA_SET")
    db = Variable.get("MONGO_DB_DATABASE_NAME")
    host = Variable.get("MONGO_DB_HOST")

    @task(task_id="restaurants_load")
    def load_restaurants():
        # Инициализируем класс, в котором реализована логика сохранения.
        restaurants_saver = RestaurantsSaver()

        # Инициализируем подключение у MongoDB.
        mongo_connect = MongoConnect(cert_path, db_user, db_pw, host, rs, db, db)

        # Инициализируем класс, реализующий чтение данных из источника.
        collection_reader = RestaurantReader(mongo_connect)

        # Инициализируем класс, в котором реализована бизнес-логика загрузки данных.
        loader = RestaurantLoader(collection_reader, dwh_pg_connect, restaurants_saver, log)

        # Запускаем копирование данных.
        loader.run_copy()

    restaurant_loader = load_restaurants()

    @task(task_id="users_load")
    def load_users():
        # Инициализируем класс, в котором реализована логика сохранения.
        users_saver = UsersSaver()

        # Инициализируем подключение у MongoDB.
        mongo_connect = MongoConnect(cert_path, db_user, db_pw, host, rs, db, db)

        # Инициализируем класс, реализующий чтение данных из источника.
        collection_reader = UsersReader(mongo_connect)

        # Инициализируем класс, в котором реализована бизнес-логика загрузки данных.
        loader = UsersLoader(collection_reader, dwh_pg_connect, users_saver, log)

        # Запускаем копирование данных.
        loader.run_copy()

    users_loader = load_users()

    @task(task_id="orders_load")
    def load_orders():
        # Инициализируем класс, в котором реализована логика сохранения.
        orders_saver = OrdersSaver()

        # Инициализируем подключение у MongoDB.
        mongo_connect = MongoConnect(cert_path, db_user, db_pw, host, rs, db, db)

        # Инициализируем класс, реализующий чтение данных из источника.
        collection_reader = OrdersReader(mongo_connect)

        # Инициализируем класс, в котором реализована бизнес-логика загрузки данных.
        loader = OrdersLoader(collection_reader, dwh_pg_connect, orders_saver, log)

        # Запускаем копирование данных.
        loader.run_copy()

    orders_loader = load_orders()

    # Задаем порядок выполнения. Таск только один, поэтому зависимостей нет.
    restaurant_loader >> users_loader >> orders_loader # type: ignore


mongo_dag_f = mongo_dag()  # noqa
