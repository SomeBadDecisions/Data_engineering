from logging import Logger
from typing import List

from examples.dds.dds_settings_repository import EtlSetting, DdsEtlSettingsRepository  
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel


class ProductsObj(BaseModel):
    id: int 
    restaurant_id: int
    product_id: str
    product_name: str
    product_price: float 
    active_from: object 
    active_to: object  


class ProductsOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_products(self, product_threshold: int, limit: int) -> List[ProductsObj]:
        with self._db.client().cursor(row_factory=class_row(ProductsObj)) as cur:
            cur.execute(
                """
                    WITH p as ( 
                    select 
                    id
                    ,object_id restaurant_oid
                    ,json_array_elements(object_value::json -> 'menu') product_json
                    , update_ts 
                    from stg.ordersystem_restaurants
                    WHERE id > %(threshold)s --Пропускаем те объекты, которые уже загрузили.
                    ORDER BY id ASC --Обязательна сортировка по id, т.к. id используем в качестве курсора.
                    LIMIT %(limit)s --Обрабатываем только одну пачку объектов.
                    )

                    select 
                    p.id
                    ,r.id restaurant_id 
                    , (p.product_json ->> '_id')::varchar product_id
                    , (p.product_json ->> 'name')::varchar product_name
                    , (p.product_json ->> 'price')::numeric(14,5) product_price
                    , p.update_ts::timestamp(0) as active_from  
                    ,'2099-12-31' as active_to
                    from p p
                    join dds.dm_restaurants r on (p.restaurant_oid  = r.restaurant_id)
                """, {
                    "threshold": product_threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs


class ProductDestRepository:

    def insert_product(self, conn: Connection, product: ProductsObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_products(restaurant_id, product_id, product_name, product_price, active_from, active_to)
                    VALUES (%(restaurant_id)s, %(product_id)s, %(product_name)s, %(product_price)s,  %(active_from)s, %(active_to)s)
                    ;
                """,
                {
                    "restaurant_id": product.restaurant_id,
                    "product_id": product.product_id,
                    "product_name": product.product_name,
                    "product_price": product.product_price,
                    "active_from": product.active_from,
                    "active_to": product.active_to
                },
            )


class ProductLoader:
    WF_KEY = "dm_products_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 100000

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = ProductsOriginRepository(pg_origin)
        self.dds = ProductDestRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log

    def load_products(self):
        # открываем транзакцию.
        # Транзакция будет закоммичена, если код в блоке with пройдет успешно (т.е. без ошибок).
        # Если возникнет ошибка, произойдет откат изменений (rollback транзакции).
        with self.pg_dest.connection() as conn:

            # Прочитываем состояние загрузки
            # Если настройки еще нет, заводим ее.
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})

            # Вычитываем очередную пачку объектов.
            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.origin.list_products(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} records to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for record in load_queue:
                self.dds.insert_product(conn, record)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
