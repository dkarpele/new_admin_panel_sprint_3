from time import sleep

import logging.config
import elasticsearch.exceptions
import elasticsearch
import logging.config
from functools import wraps

import psycopg2
import psycopg2.extensions
from psycopg2.extras import DictCursor, LoggingConnection

from consts import START_SLEEP_TIME, FACTOR, BORDER_SLEEP_TIME

logging.config.fileConfig(fname='logger.conf', disable_existing_loggers=False)

# Get the logger specified in the file
logger = logging.getLogger(__name__)


def db_cursor_backoff(conn_info, db_type='pg'):
    """Wrap function to set up and tear down a Postgres connection while
    providing a cursor object to make queries with.
    Backoff process.
    """
    def wrap(f):
        @wraps(f)
        def wrapper(self, *args, **kwargs):
            wait_time = 0
            n = 0
            while True:
                try:
                    if db_type == 'es':
                        conn_type = elasticsearch.Elasticsearch(**conn_info)
                    elif db_type == 'pg':
                        conn_type = psycopg2.connect(**conn_info,
                                                     cursor_factory=DictCursor,
                                                     connection_factory=LoggingConnection)
                    else:
                        logging.error(f'DB type {db_type} doesn\'t exist!')
                        exit(1)
                    with conn_type as connection:
                        if db_type == 'pg':
                            connection.initialize(logger)
                        if db_type == 'pg':
                            with connection.cursor() as cursor:
                                # Call function passing in cursor
                                return_val = f(self, cursor, connection, *args,
                                               **kwargs)
                        else:
                            return_val = f(self, connection, *args,
                                           **kwargs)
                        if n > 0:
                            wait_time = START_SLEEP_TIME * FACTOR ** n
                            n -= 1
                            sleep(wait_time)
                        elif n == 0:
                            wait_time = 0
                        if db_type == 'pg':
                            if connection.info.status == 0:
                                break
                        elif db_type == 'es':
                            if connection.info():
                                break
                except Exception as err:
                    logging.error(err)
                    return_val = None
                    if wait_time < BORDER_SLEEP_TIME:
                        wait_time = START_SLEEP_TIME * FACTOR ** n
                        n += 1
                    elif wait_time >= BORDER_SLEEP_TIME:
                        wait_time = BORDER_SLEEP_TIME
                    sleep(wait_time)
                finally:
                    connection.close()

            return return_val
        return wrapper
    return wrap
