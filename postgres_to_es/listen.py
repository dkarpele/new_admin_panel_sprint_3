import json
import select
import logging.config

import psycopg2.extensions
import psycopg2
import psycopg2.errors
from psycopg2.extras import DictCursor
from psycopg2.extras import LoggingConnection

from etl import ETL
from load import Load
from pg_sql_request import func_notify_trigger, trigger_insert_update,\
    listen_event, initial_notify, payload
from config import DBCreds, DATABASE_LIST
from tools import db_cursor_backoff


logging.config.fileConfig(fname='logger.conf', disable_existing_loggers=False)

# Get the logger specified in the file
logger = logging.getLogger(__name__)


class PGListen:
    def __init__(self):
        self.event_name = 'db_event'

    @db_cursor_backoff(DBCreds().dict(), db_type='pg')
    def create_triggers(self, cursor, connection) -> None:
        """ Create triggers to be notified by the change in `modified` field"""
        # Create triggers only for tables filmwork, person, genre!
        for base in list(DATABASE_LIST.keys())[:3]:
            func_name = f"notify_trigger_{base}"
            try:
                cursor.execute(func_notify_trigger(func_name,
                                                   base,
                                                   self.event_name))
                cursor.execute(trigger_insert_update(func_name,
                                                     base))
            except psycopg2.errors.lookup('09000') as err:
                logging.error(f'Failed to create trigger {base}_notify')
                logging.error(err)
            logging.info(f'Trigger {base}_notify created.')

    def listen_events(self) -> None:
        try:
            connection = psycopg2.connect(**DBCreds().dict(),
                                          cursor_factory=DictCursor,
                                          connection_factory=LoggingConnection)
            connection.initialize(logger)
            cursor = connection.cursor()
        except psycopg2.OperationalError as err:
            logging.error('Postgres is unavailable. Exiting.')
            logging.error(err)
            exit(1)

        try:
            connection.set_isolation_level(
                psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

            cursor.execute(listen_event(self.event_name))

            logging.info(f"Waiting for notifications on channel "
                         f"'{self.event_name}'")

            # Initial notify. Start to create ES index from here for tables
            # `film_work`, `person`, `genre`.
            # base = `film_work`
            for base in list(DATABASE_LIST.keys())[:3]:
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
            cursor.close()
            connection.close()


if __name__ == '__main__':
    Load().es_create_index_if_not_exists()
    PGListen().create_triggers()
    PGListen().listen_events()
