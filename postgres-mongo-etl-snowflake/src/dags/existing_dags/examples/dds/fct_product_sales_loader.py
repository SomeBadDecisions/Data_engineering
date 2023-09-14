from logging import Logger
from typing import List

from examples.dds.dds_settings_repository import EtlSetting, DdsEtlSettingsRepository  
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel


class SalesObj(BaseModel):
    id: int 
    product_id: int
    order_id: int
    count: int 
    price: float 
    total_sum: float 
    bonus_payment: float 
    bonus_grant: float      


class SalesOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_sales(self, sales_threshold: int, limit: int) -> List[SalesObj]:
        with self._db.client().cursor(row_factory=class_row(SalesObj)) as cur:
            cur.execute(
                """
                    with t as (
SELECT           id 
                ,event_ts --event_type, event_value
                ,event_value ::json->>'order_id' as order_id
                ,json_array_elements(event_value ::json->'product_payments')::json->>'product_id' as product_id
                ,json_array_elements(event_value ::json->'product_payments')::json->>'price' as price
                ,json_array_elements(event_value ::json->'product_payments')::json->>'quantity' as count
                ,json_array_elements(event_value ::json->'product_payments')::json->>'product_cost' as total_sum
                ,json_array_elements(event_value ::json->'product_payments')::json->>'bonus_payment' as bonus_payment
                ,json_array_elements(event_value ::json->'product_payments')::json->>'bonus_grant' as bonus_grant
                FROM stg.bonussystem_events
                where event_type ='bonus_transaction'
                    and id > %(threshold)s --Пропускаем те объекты, которые уже загрузили.
                    ORDER BY id ASC --Обязательна сортировка по id, т.к. id используем в качестве курсора.
                    LIMIT %(limit)s --Обрабатываем только одну пачку объектов.
                    )

                    select t.id 
  ,p.id as product_id
  ,do2.id as order_id
  ,t.count::int
  ,t.price::numeric(14, 2) 
  ,t.total_sum::numeric(14, 2)
  ,t.bonus_payment::numeric(14, 2)
  ,t.bonus_grant::numeric(14, 2)
from t 
join dds.dm_orders do2 on do2.order_key =t.order_id
join dds.dm_products p on p.product_id =t.product_id
                """, {
                    "threshold": sales_threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs


class SalesDestRepository:

    def insert_sales(self, conn: Connection, sales: SalesObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.fct_product_sales(product_id, order_id, count, price, total_sum, bonus_payment, bonus_grant)
                    VALUES (%(product_id)s, %(order_id)s, %(count)s, %(price)s,  %(total_sum)s, %(bonus_payment)s, %(bonus_grant)s)
                    ;
                """,
                {
                    "product_id": sales.product_id,
                    "order_id": sales.order_id,
                    "count": sales.count,
                    "price": sales.price,
                    "total_sum": sales.total_sum,
                    "bonus_payment": sales.bonus_payment,
                    "bonus_grant": sales.bonus_grant
                },
            )


class SalesLoader:
    WF_KEY = "fct_product_sales_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 100000

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = SalesOriginRepository(pg_origin)
        self.dds = SalesDestRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log

    def load_sales(self):
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
            load_queue = self.origin.list_sales(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} records to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for record in load_queue:
                self.dds.insert_sales(conn, record)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
