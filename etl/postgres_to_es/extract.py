import abc
import json
import logging.config

import psycopg2
from psycopg2.extras import DictCursor, LoggingConnection
from psycopg2 import errors

from schemas import DSL, CHUNK_SIZE
from state import JsonFileStorage, State
from pg_sql_request import filmwork_merger
logging.config.fileConfig(fname='logger.conf', disable_existing_loggers=False)

# Get the logger specified in the file
logger = logging.getLogger(__name__)


class Extract(abc.ABC):
    def __init__(self, key, old_state, new_state):
        self.key = key
        self.old_state = old_state
        self.new_state = new_state
        self.pg_conn = psycopg2.connect(**DSL,
                                        cursor_factory=DictCursor,
                                        connection_factory=LoggingConnection)

    @staticmethod
    def db_exception_handler(func):
        def inner_function(self):
            res = []
            try:
                res = func(self)
            except (errors.lookup("22P02"),
                    errors.lookup("25P02"),
                    errors.lookup("42601")) as err:
                logger.error(err)
            finally:
                self.pg_conn.close()
                return res
        return inner_function

    @db_exception_handler
    def extract(self):
        with self.pg_conn as pg_conn:
            pg_conn.initialize(logger)
            with pg_conn.cursor() as cursor:
                self.producer(cursor)
                self.enricher(cursor)
                sm = self.merger(cursor)
        return sm

    @abc.abstractmethod
    def producer(self, cursor):
        pass

    @abc.abstractmethod
    def enricher(self, cursor):
        pass

    @abc.abstractmethod
    def merger(self, cursor):
        pass


class Person(Extract):
    def __init__(self, key, old_state, new_state):
        super().__init__(key, old_state, new_state)

    def producer(self, cursor):
        pass

    def enricher(self, cursor):
        pass

    def merger(self, cursor):
        pass


class Genre(Extract):
    def __init__(self, key, old_state, new_state):
        super().__init__(key, old_state, new_state)

    def producer(self, cursor):
        pass

    def enricher(self, cursor):
        pass

    def merger(self, cursor):
        pass


class Filmwork(Extract):
    def __init__(self, key, old_state, new_state):
        super().__init__(key, old_state, new_state)

    def producer(self, cursor):
        pass

    def enricher(self, cursor):
        pass

    def merger(self, cursor):
        cursor.execute(filmwork_merger(self.old_state['modified']))

        def load():
            while True:
                rows = cursor.fetchmany(size=CHUNK_SIZE)
                if not rows:
                    return
                yield from rows
        data = load()
        return [elem for elem in data]
