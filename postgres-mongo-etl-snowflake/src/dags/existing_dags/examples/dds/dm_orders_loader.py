from logging import Logger
from typing import List

from examples.dds.dds_settings_repository import EtlSetting, DdsEtlSettingsRepository  
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel


class OrdersObj(BaseModel):
    id: int 
    user_id: int
    restaurant_id: int
    timestamp_id: int 
    order_key: str 
    order_status: str   


class OrdersOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_orders(self, order_threshold: int, limit: int) -> List[OrdersObj]:
        with self._db.client().cursor(row_factory=class_row(OrdersObj)) as cur:
            cur.execute(
                """
                    with pre as (                   
                   select 
                        id
                        ,object_id order_key
                        , object_value::json ->> 'final_status' order_status
                        , object_value::json->'restaurant'->>'id' restaurant_id  
                        , cast(object_value::json->>'date' as timestamp) ts
                        , object_value::json->'user'->> 'id'  user_id
                    from stg.ordersystem_orders
                    WHERE id > %(threshold)s --Пропускаем те объекты, которые уже загрузили.
                    ORDER BY id ASC --Обязательна сортировка по id, т.к. id используем в качестве курсора.
                    LIMIT %(limit)s --Обрабатываем только одну пачку объектов.
                    )

                    select 
                p.id
                ,u.id user_id
                , r.id restaurant_id
                , t.id timestamp_id
                , p.order_key order_key
                , p.order_status order_status
                    from pre p
                    inner join dds.dm_restaurants r 
                        on p.restaurant_id = r.restaurant_id 
                    inner join dds.dm_users u
                        on p.user_id = u.user_id
                    inner join dds.dm_timestamps t 
                on p.ts = t.ts
                """, {
                    "threshold": order_threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs


class OrdersDestRepository:

    def insert_order(self, conn: Connection, order: OrdersObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_orders(user_id, restaurant_id, timestamp_id, order_key, order_status)
                    VALUES (%(user_id)s, %(restaurant_id)s, %(timestamp_id)s, %(order_key)s,  %(order_status)s)
                    ;
                """,
                {
                    "user_id": order.user_id,
                    "restaurant_id": order.restaurant_id,
                    "timestamp_id": order.timestamp_id,
                    "order_key": order.order_key,
                    "order_status": order.order_status
                },
            )


class OrderLoader:
    WF_KEY = "dm_orders_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 100000

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = OrdersOriginRepository(pg_origin)
        self.dds = OrdersDestRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log

    def load_orders(self):
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
            load_queue = self.origin.list_orders(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} records to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for record in load_queue:
                self.dds.insert_order(conn, record)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
