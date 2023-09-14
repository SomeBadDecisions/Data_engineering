from datetime import datetime
from logging import Logger
from typing import Dict, List

from lib.kafka_connect import KafkaConsumer, KafkaProducer

from dds_loader.repository.dds_repository import DdsRepository, OrderDdsBuilder


class DdsMessageProcessor:
    def __init__(self,
                 consumer: KafkaConsumer,
                 producer: KafkaProducer,
                 dds: DdsRepository,
                 logger: Logger) -> None:
        self._consumer = consumer
        self._producer = producer
        self._repository = dds

        self._logger = logger

        self._batch_size = 30

    def run(self) -> None:
        self._logger.info(f"{datetime.utcnow()}: START")

        for _ in range(self._batch_size):
            msg = self._consumer.consume()
            if not msg:
                self._logger.info(f"{datetime.utcnow()}: NO messages. Quitting.")
                break

            self._logger.info(f"{datetime.utcnow()}: {msg}")

            order_dict = msg['payload']
            builder = OrderDdsBuilder(order_dict)

            self._load_hubs(builder)
            self._load_links(builder)
            self._load_sats(builder)

            dst_msg = {
                "object_id": str(builder.h_order().h_order_pk),
                "sent_dttm": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                "object_type": "order_report",
                "payload": {
                    "id": str(builder.h_order().h_order_pk),
                    "order_dt": builder.h_order().order_dt.strftime("%Y-%m-%d %H:%M:%S"),
                    "status": builder.s_order_status().status,
                    "restaurant": {
                        "id": str(builder.h_restaurant().h_restaurant_pk),
                        "name": builder.s_restaurant_names().name
                    },
                    "user": {
                        "id": str(builder.h_user().h_user_pk),
                        "username": builder.s_user_names().username
                    },
                    "products": self._format_products(builder)
                }
            }

            self._logger.info(f"{datetime.utcnow()}: {dst_msg}")
            self._producer.produce(dst_msg)

        self._logger.info(f"{datetime.utcnow()}: FINISH")

    def _load_hubs(self, builder: OrderDdsBuilder) -> None:

        self._repository.h_user_insert(builder.h_user())

        for p in builder.h_product():
            self._repository.h_product_insert(p)

        for c in builder.h_category():
            self._repository.h_category_insert(c)

        self._repository.h_restaurant_insert(builder.h_restaurant())

        self._repository.h_order_insert(builder.h_order())

    def _load_links(self, builder: OrderDdsBuilder) -> None:
        self._repository.l_order_user_insert(builder.l_order_user())

        for op_link in builder.l_order_product():
            self._repository.l_order_product_insert(op_link)

        for pr_link in builder.l_product_restaurant():
            self._repository.l_product_restaurant_insert(pr_link)

        for pc_link in builder.l_product_category():
            self._repository.l_product_category_insert(pc_link)

    def _load_sats(self, builder: OrderDdsBuilder) -> None:
        self._repository.s_order_cost_insert(builder.s_order_cost())
        self._repository.s_order_status_insert(builder.s_order_status())
        self._repository.s_restaurant_names_insert(builder.s_restaurant_names())
        self._repository.s_user_names_insert(builder.s_user_names())

        for pn in builder.s_product_names():
            self._repository.s_product_names_insert(pn)

    def _format_products(self, builder: OrderDdsBuilder) -> List[Dict]:
        products = []

        p_names = {x.h_product_pk: x.name for x in builder.s_product_names()}

        cat_names = {x.h_category_pk: {"id": str(x.h_category_pk), "name": x.category_name} for x in builder.h_category()}
        prod_cats = {x.h_product_pk: cat_names[x.h_category_pk] for x in builder.l_product_category()}

        for p in builder.h_product():
            msg_prod = {
                "id": str(p.h_product_pk),
                "name": p_names[p.h_product_pk],
                "category": prod_cats[p.h_product_pk]
            }

            products.append(msg_prod)

        return products
