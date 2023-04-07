import json
import select
import os
import logging.config
import psycopg2.extensions
import psycopg2

from psycopg2 import errors
from psycopg2.extensions import connection as _connection
from psycopg2.extras import DictCursor
from psycopg2.extras import LoggingConnection
from dotenv import load_dotenv

from pg_sql_request import func_notify_trigger, trigger_insert_update, listen_event
from schemas import DSL, DATABASE_LIST, STATE_FILE
from state import JsonFileStorage, State
from load import Load

load_dotenv()

logging.config.fileConfig(fname='logger.conf', disable_existing_loggers=False)

# Get the logger specified in the file
logger = logging.getLogger(__name__)


class PGListen:
    # @contextmanager
    # def conn_context(self):
    #     self.connection.initialize(logger)
    #     try:
    #         yield self.connection.cursor()
    #     except (errors.lookup("22P02"), errors.lookup("25P02")) as err:
    #         logger.error(err)

    event_name = 'db_event'

    # TODO create decorator for exceptions
    def create_triggers(self) -> None:
        """ Create triggers to be notified by the change in `modified` field"""
        pg_conn = psycopg2.connect(**DSL,
                                   cursor_factory=DictCursor,
                                   connection_factory=LoggingConnection)
        try:
            with pg_conn as pg_conn:
                pg_conn.initialize(logger)
                with pg_conn.cursor() as cursor:
                    # Create triggers only for tables filmwork, person, genre!
                    for base in list(DATABASE_LIST.keys())[:3]:
                        func_name = f"notify_trigger_{base}"
                        cursor.execute(func_notify_trigger(func_name,
                                                           base,
                                                           self.event_name))
                        cursor.execute(trigger_insert_update(func_name,
                                                             base))

        except (errors.lookup("22P02"), errors.lookup("25P02")) as err:
            logger.error(err)
        finally:
            pg_conn.close()

    def listen_events(self) -> None:
        pg_conn = psycopg2.connect(**DSL,
                                   cursor_factory=DictCursor,
                                   connection_factory=LoggingConnection)
        try:
            pg_conn.initialize(logger)
            pg_conn.set_isolation_level(
                psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

            curs = pg_conn.cursor()
            curs.execute(listen_event(self.event_name))

            logging.info(f"Waiting for notifications on channel "
                         f"'{self.event_name}'")
            while True:
                try:
                    if select.select([pg_conn], [], [], 5) == ([], [], []):
                        logging.info("Timeout")
                except KeyboardInterrupt:
                    logger.info(f'Process stopped by user')
                    return
                else:
                    pg_conn.poll()
                    while pg_conn.notifies:
                        notify = pg_conn.notifies.pop(0)
                        logger.info(f"Got NOTIFY: {notify.pid}, "
                                    f"{notify.channel} {notify.payload}")
                        state = State(JsonFileStorage(STATE_FILE))

                        for base, state in json.loads(notify.payload).items():
                            Load(base, state)
                            # state.set_state(key, value)
        except (errors.lookup("22P02"), errors.lookup("25P02")) as err:
            logger.error(err)
        finally:
            pg_conn.close()


if __name__ == '__main__':
    pg_listen = PGListen()
    pg_listen.create_triggers()
    pg_listen.listen_events()
