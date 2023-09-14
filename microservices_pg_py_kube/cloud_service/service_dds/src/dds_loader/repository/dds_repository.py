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
