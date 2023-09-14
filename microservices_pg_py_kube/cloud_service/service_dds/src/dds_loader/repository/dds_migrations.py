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
