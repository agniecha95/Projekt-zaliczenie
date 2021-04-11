from my_logger import logger
import os
import sqlite3
import time

class Database:
    def __init__(self, db_name):
        self.db_name = db_name
        self.conn = self.connect(tries=3, delay=0.1, db_name=db_name)

    def connection_retry(fun):
        def wrapper(**kwargs):
            tries = kwargs['tries']
            delay = kwargs['delay']
            db_name = kwargs['db_name']

            for i in range(int(tries)):
                logger.info(f"It is my {i} try to connect")
                conn = None
                if os.path.isfile(db_name):
                    try:
                        conn = sqlite3.connect(db_name)
                    except Exception as e:
                        logger.error(f"The {i} attempt failed", exc_info=True)
                        time.sleep(delay)
                    else:
                        logger.info("Connection established")
                    return conn
                else:
                    logger.error(f"No database {db_name}")

        return wrapper

    @staticmethod
    @connection_retry
    def connect():
        pass
