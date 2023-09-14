import logging

from apscheduler.schedulers.background import BackgroundScheduler
from flask import Flask

from app_config import AppConfig
from stg_loader.stg_message_processor_job import StgMessageProcessor
from stg_loader.repository.stg_repository import StgRepository

app = Flask(__name__)

if __name__ == '__main__':
    
    app.logger.setLevel(logging.DEBUG)

    config = AppConfig()

    proc = StgMessageProcessor(consumer=config.kafka_consumer(),
                 producer=config.kafka_producer(),
                 redis=config.redis_client(),
                 stg_repository=StgRepository(config.pg_warehouse_db()),
                 batch_size=100,
                 logger=app.logger)

   
    scheduler = BackgroundScheduler()
    scheduler.add_job(func=proc.run, trigger="interval", seconds=config.DEFAULT_JOB_INTERVAL)
    scheduler.start()

    app.run(debug=True, host='0.0.0.0', use_reloader=False)
