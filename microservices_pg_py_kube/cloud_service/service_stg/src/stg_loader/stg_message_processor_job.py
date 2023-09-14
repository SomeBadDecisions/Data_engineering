import time
from lib.kafka_connect.kafka_connectors import KafkaConsumer
from lib.kafka_connect.kafka_connectors import KafkaProducer
from stg_loader.repository.stg_repository import StgRepository 
from lib.redis.redis_client import RedisClient
from datetime import datetime
from logging import Logger


class StgMessageProcessor:
    def __init__ (self,
                  consumer: KafkaConsumer,
                  producer: KafkaProducer,
                  redis: RedisClient,
                  stg_repository: StgRepository,
                  batch_size: int,
                  logger: Logger) -> None:

        self._consumer = consumer 
        self._producer = producer 
        self._redis = redis 
        self._stg_repository = stg_repository 
        self._batch_size = batch_size
        self._logger = logger 


    def run(self) -> None:
        self._logger.info(f"{datetime.utcnow()}: START")

        for _ in range(self._batch_size):
            msg = self._consumer.consume()
            if not msg:
                break

            self._logger.info(f"{datetime.utcnow()}: Message received")

            order = msg['payload']
            self._stg_repository.order_events_insert(
                msg["object_id"],
                msg["object_type"],
                msg["sent_dttm"],
                json.dumps(order))

            user_id = order["user"]["id"]
            user = self._redis.get(user_id)
            user_name = user["name"]
            
            restaurant_id = order['restaurant']['id']
            restaurant = self._redis.get(restaurant_id)
            restaurant_name = restaurant["name"]
            
            dst_msg = {
                "object_id": msg["object_id"],
                "object_type": "order",
                "payload": {
                    "id": msg["object_id"],
                    "date": order["date"],
                    "cost": order["cost"],
                    "payment": order["payment"],
                    "status": order["final_status"],
                    "restaurant": self._format_restaurant(restaurant_id, restaurant_name),
                    "user": self._format_user(user_id, user_name),
                    "products": self._format_items(order["order_items"], restaurant)
                }
            }

            self._producer.produce(dst_msg)
            self._logger.info(f"{datetime.utcnow()}. Message Sent")

        self._logger.info(f"{datetime.utcnow()}: FINISH")

    def _format_restaurant(self, id, name) -> dict[str, str]:
        return {
            "id": id,
            "name": name
        }

    def _format_user(self, id, name) -> dict[str, str]:
        return {
            "id": id,
            "name": name
        }

    def _format_items(self, order_items, restaurant) -> list[dict[str, str]]:
        items = []

        menu = restaurant["menu"]
        for it in order_items:
            menu_item = next(x for x in menu if x["_id"] == it["id"])
            dst_it = {
                "id": it["id"],
                "price": it["price"],
                "quantity": it["quantity"],
                "name": menu_item["name"],
                "category": menu_item["category"]
            }
            items.append(dst_it)

        return items
