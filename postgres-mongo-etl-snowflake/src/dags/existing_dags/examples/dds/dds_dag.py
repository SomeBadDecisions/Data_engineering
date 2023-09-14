import logging

import pendulum
from airflow.decorators import dag, task
from examples.dds.dm_users_loader import UserLoader
from examples.dds.dm_restaurants_loader import RestaurantLoader
from examples.dds.dm_timestamps_loader import TsLoader
from examples.dds.dm_products_loader import ProductLoader
from examples.dds.dm_orders_loader import OrderLoader
from examples.dds.fct_product_sales_loader import SalesLoader
from lib import ConnectionBuilder

log = logging.getLogger(__name__)


@dag(
    schedule_interval='0/15 * * * *',  # Задаем расписание выполнения дага - каждый 15 минут.
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['sprint5', 'stg', 'origin', 'example'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=True  # Остановлен/запущен при появлении. Сразу запущен.
)
def dds_dag():
    # Создаем подключение к базе dwh.
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    # Объявляем таск, который загружает данные.
    @task(task_id="dm_users_load")
    def load_dm_users():
        # создаем экземпляр класса, в котором реализована логика.
        user_loader = UserLoader(dwh_pg_connect, dwh_pg_connect, log)
        user_loader.load_users()  # Вызываем функцию, которая перельет данные.

    # Инициализируем объявленные таски.
    dm_users = load_dm_users()

    @task(task_id="dm_restaurants_load")
    def load_dm_restaurants():
        # создаем экземпляр класса, в котором реализована логика.
        restaurant_loader = RestaurantLoader(dwh_pg_connect, dwh_pg_connect, log)
        restaurant_loader.load_restaurants()  # Вызываем функцию, которая перельет данные.

    # Инициализируем объявленные таски.
    dm_restaurants = load_dm_restaurants()

    @task(task_id="dm_timestamps_load")
    def load_dm_timestamps():
        # создаем экземпляр класса, в котором реализована логика.
        ts_loader = TsLoader(dwh_pg_connect, dwh_pg_connect, log)
        ts_loader.load_ts()  # Вызываем функцию, которая перельет данные.

       # Инициализируем объявленные таски.
    dm_timestamps = load_dm_timestamps()
    
    # Объявляем таск, который загружает данные.
    @task(task_id="dm_products_load")
    def load_dm_products():
        # создаем экземпляр класса, в котором реализована логика.
        product_loader = ProductLoader(dwh_pg_connect, dwh_pg_connect, log)
        product_loader.load_products()  # Вызываем функцию, которая перельет данные.

    # Инициализируем объявленные таски.
    dm_products = load_dm_products()

     # Объявляем таск, который загружает данные.
    @task(task_id="dm_orders_load")
    def load_dm_orders():
        # создаем экземпляр класса, в котором реализована логика.
        product_loader = OrderLoader(dwh_pg_connect, dwh_pg_connect, log)
        product_loader.load_orders()  # Вызываем функцию, которая перельет данные.

    # Инициализируем объявленные таски.
    dm_orders = load_dm_orders()

      # Объявляем таск, который загружает данные.
    @task(task_id="sales_load")
    def load_sales():
        # создаем экземпляр класса, в котором реализована логика.
        sales_loader = SalesLoader(dwh_pg_connect, dwh_pg_connect, log)
        sales_loader.load_sales()  # Вызываем функцию, которая перельет данные.

    # Инициализируем объявленные таски.
    fct_sales = load_sales()

    dm_users >> dm_restaurants >> dm_timestamps >> dm_products >> dm_orders >> fct_sales   


dds_dag_f = dds_dag()
