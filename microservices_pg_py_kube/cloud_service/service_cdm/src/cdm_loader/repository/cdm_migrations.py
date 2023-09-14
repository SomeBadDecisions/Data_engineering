from lib.pg import PgConnect
from psycopg import Cursor


class Migrator:
    def __init__(self, db: PgConnect) -> None:
        self._db = db

    def up(self) -> None:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                self._schema(cur)
                self._user_product_counters_create(cur)
                self._user_category_counters_create(cur)
                self._restaurant_category_counters_create(cur)

    def _schema(self, cursor: Cursor) -> None:
        cursor.execute(
            """
                CREATE SCHEMA IF NOT EXISTS cdm;
            """
        )

    def _user_product_counters_create(self, cursor: Cursor) -> None:
        cursor.execute(
            """
                CREATE TABLE IF NOT EXISTS cdm.user_product_counters(
                    id INT NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
                    user_id UUID NOT NULL,
                    product_id UUID NOT NULL,
                    product_name VARCHAR NOT NULL,
                    order_cnt INT NOT NULL DEFAULT(0) CHECK(order_cnt >= 0)
                );

                CREATE UNIQUE INDEX IF NOT EXISTS IDX_user_product_counters__user_id_product_id
                ON cdm.user_product_counters (user_id, product_id);
            """
        )

    def _user_category_counters_create(self, cursor: Cursor) -> None:
        cursor.execute(
            """
                CREATE TABLE IF NOT EXISTS cdm.user_category_counters(
                    id INT NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
                    user_id UUID NOT NULL,
                    category_id UUID NOT NULL,
                    category_name VARCHAR NOT NULL,
                    order_cnt INT NOT NULL DEFAULT(0) CHECK(order_cnt >= 0)
                );

                CREATE UNIQUE INDEX IF NOT EXISTS IDX_user_category_counters__user_id_category_id
                ON cdm.user_category_counters (user_id, category_id);
            """
        )

    def _restaurant_category_counters_create(self, cursor: Cursor) -> None:
        cursor.execute(
            """
                CREATE TABLE IF NOT EXISTS cdm.restaurant_category_counters(
                    id INT NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
                    restaurant_id UUID NOT NULL,
                    restaurant_name VARCHAR NOT NULL,
                    category_id UUID NOT NULL,
                    category_name VARCHAR NOT NULL,
                    order_cnt INT NOT NULL DEFAULT(0) CHECK(order_cnt >= 0)
                );

                CREATE UNIQUE INDEX IF NOT EXISTS IDX_restaurant_category_counters__restaurant_id_category_id
                ON cdm.restaurant_category_counters (restaurant_id, category_id);
            """
        )
