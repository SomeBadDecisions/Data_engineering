from datetime import datetime
from logging import Logger
from typing import List

from examples.dds.dds_settings_repository import EtlSetting, DdsEtlSettingsRepository  
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel


class TsObj(BaseModel):
    id: int
    ts: object
    year: int 
    month: int  
    day: int
    date: object
    time: object   


class TsOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_ts(self, ts_threshold: int, limit: int) -> List[TsObj]:
        with self._db.client().cursor(row_factory=class_row(TsObj)) as cur:
            cur.execute(
                """
                    select id
                    ,DATE_TRUNC('second', update_ts) as ts
                    ,EXTRACT(year from update_ts) as year
                    ,EXTRACT(month from update_ts) as month
                    ,EXTRACT(day from update_ts) as day
                    ,Date_trunc('second', update_ts)::time as time 
                    ,update_ts::date as date
                    from stg.ordersystem_orders
                    WHERE id > %(threshold)s --Пропускаем те объекты, которые уже загрузили.
                    ORDER BY id ASC --Обязательна сортировка по id, т.к. id используем в качестве курсора.
                    LIMIT %(limit)s; --Обрабатываем только одну пачку объектов.
                """, {
                    "threshold": ts_threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs


class TsDestRepository:

    def insert_ts(self, conn: Connection, ts: TsObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_timestamps(ts, year, month, day, time, date)
                    VALUES (%(ts)s, %(year)s, %(month)s, %(day)s, %(time)s, %(date)s)
                    ;
                """,
                {
                    "ts": ts.ts,
                    "year": ts.year,
                    "month": ts.month,
                    "day": ts.day,
                    "time": ts.time,
                    "date": ts.date 
                },
            )


class TsLoader:
    WF_KEY = "dm_ts_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 100000

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = TsOriginRepository(pg_origin)
        self.dds = TsDestRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log

    def load_ts(self):
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
            load_queue = self.origin.list_ts(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} records to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for record in load_queue:
                self.dds.insert_ts(conn, record)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
