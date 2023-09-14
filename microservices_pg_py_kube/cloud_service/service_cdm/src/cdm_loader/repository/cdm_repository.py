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
