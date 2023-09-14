import logging

from apscheduler.schedulers.background import BackgroundScheduler
from flask import Flask

from app_config import AppConfig
from dds_loader.dds_message_processor_job import DdsMessageProcessor

app = Flask(__name__)

config = AppConfig()

if __name__ == '__main__':
    app.logger.setLevel(logging.DEBUG)

    migrator = DdsMigrator(config.pg_warehouse_db())
    migrator.up()

    proc = DdsMessageProcessor(
        config.kafka_consumer(),
        config.kafka_producer(),
        DdsRepository(config.pg_warehouse_db()),
        app.logger
    )

    scheduler = BackgroundScheduler()
    scheduler.add_job(func=proc.run, trigger="interval", seconds=25)
    scheduler.start()

    app.run(debug=True, host='0.0.0.0', use_reloader=False)
