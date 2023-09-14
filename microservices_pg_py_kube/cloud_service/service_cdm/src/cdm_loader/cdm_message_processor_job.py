from datetime import datetime
from logging import Logger
from uuid import UUID

from lib.kafka_connect import KafkaConsumer

from cdm_loader.repository import (RestaurantCategoryCounterRepository,
                                   UserCategoryCounterRepository,
                                   UserProductCounterRepository)


class CdmMessageProcessor:
    def __init__(self,
                 consumer: KafkaConsumer,
                 userproductcnt: UserProductCounterRepository,
                 usercategorycnt: UserCategoryCounterRepository,
                 restcategorycnt: RestaurantCategoryCounterRepository,
                 logger: Logger,
                 ) -> None:
        self._consumer = consumer
        self._userproductcnt = userproductcnt
        self._usercategorycnt = usercategorycnt
        self._restcategorycnt = restcategorycnt

        self._logger = logger

        self._batch_size = 3

    def run(self) -> None:
        self._logger.info(f"{datetime.utcnow()}: START")

        for _ in range(self._batch_size):
            msg = self._consumer.consume()
            if not msg:
                break

            self._logger.info(f"{datetime.utcnow()}: {msg}")

            order = msg['payload']
            if order['status'] == 'CANCELLED':
                self._logger.info(f"{datetime.utcnow()}: CANCELLED. Skipping.")
                continue

            user_id = UUID(order['user']['id'])
            cat_dict = {}
            for p in order['products']:
                prod_id = UUID(p['id'])
                prod_name = p['name']
                self._userproductcnt.inc(user_id, prod_id, prod_name)

                cat_dict[p['category']['id']] = p['category']['name']

            rest_id = UUID(order['restaurant']['id'])
            rest_name = order['restaurant']['name']
            for (cat_id, cat_name) in cat_dict.items():
                self._restcategorycnt.inc(rest_id, rest_name, UUID(cat_id), cat_name)

        self._logger.info(f"{datetime.utcnow()}: FINISH")
