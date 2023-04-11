import json
import select
import logging.config

import psycopg2.extensions
import psycopg2

from psycopg2.extras import DictCursor
from psycopg2.extras import LoggingConnection

from etl import ETL
from load import Load
from pg_sql_request import func_notify_trigger, trigger_insert_update,\
    listen_event, initial_notify, payload
from consts import DSL, DATABASE_LIST
from tools import db_cursor_backoff


logging.config.fileConfig(fname='logger.conf', disable_existing_loggers=False)

# Get the logger specified in the file
logger = logging.getLogger(__name__)


class PGListen:
    def __init__(self):
        self.event_name = 'db_event'

    @db_cursor_backoff(DSL, db_type='pg')
    def create_triggers(self, cursor, connection) -> None:
        """ Create triggers to be notified by the change in `modified` field"""
        # Create triggers only for tables filmwork, person, genre!
        for base in list(DATABASE_LIST.keys())[:3]:
            func_name = f"notify_trigger_{base}"
            cursor.execute(func_notify_trigger(func_name,
                                               base,
                                               self.event_name))
            cursor.execute(trigger_insert_update(func_name,
                                                 base))

    def listen_events(self) -> None:
        try:
            connection = psycopg2.connect(**DSL,
                                  cursor_factory=DictCursor,
                                  connection_factory=LoggingConnection)
            connection.initialize(logger)
            cursor = connection.cursor()

            connection.set_isolation_level(
                psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

            cursor.execute(listen_event(self.event_name))

            logging.info(f"Waiting for notifications on channel "
                         f"'{self.event_name}'")

            # Initial notify. Start to create index from here for table
            # `film_work`.
            base = list(DATABASE_LIST.keys())[0]
            cursor.execute(f'SELECT {payload(base)}')
            res = cursor.fetchone()
            cursor.execute(initial_notify(self.event_name,
                                          str(res[0]).replace("'", '"')))

            while True:
                try:
                    if select.select([connection], [], [], 5) == ([], [], []):
                        logging.info("Timeout. Waiting for table updates...")
                except KeyboardInterrupt:
                    logger.info(f'Process stopped by user')
                    return
                else:
                    connection.poll()

                    while connection.notifies:

                        notify = connection.notifies.pop(0)
                        logger.info(f"Got NOTIFY: {notify.pid}, "
                                    f"{notify.channel} {notify.payload}")

                        for base, new_state in \
                                json.loads(notify.payload).items():
                            ETL(base, new_state)
        except Exception as err:
            logging.error(err)
        finally:
            connection.close()


if __name__ == '__main__':
    Load().es_create_index_if_not_exists()
    PGListen().create_triggers()
    PGListen().listen_events()
