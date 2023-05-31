import abc
import logging.config

from typing import List, Any

from schemas import Genre as Genre_schema
from config import DBCreds
from pg_sql_request import genre_merger
from tools import db_cursor_backoff

logging.config.fileConfig(fname='logger.conf', disable_existing_loggers=False)

# Get the logger specified in the file
logger = logging.getLogger(__name__)


class Extract(abc.ABC):
    def __init__(self, base, old_state, start, next_):
        self.base = base
        self.old_state = old_state
        self.start = start
        self.next_ = next_

    @db_cursor_backoff(DBCreds().dict(), db_type='pg')
    def extract(self, cursor, connection) -> List[Any]:
        merger = self.merger(cursor)
        return merger

    @abc.abstractmethod
    def producer(self, cursor) -> List[Any]:
        pass

    @abc.abstractmethod
    def enricher(self, cursor) -> List[Any]:
        pass

    def merger(self, cursor) -> List[Any]:
        m = Genre(self.base,
                  self.old_state,
                  self.start,
                  self.next_)
        return m.merger(cursor)

    @staticmethod
    def load(cursor):
        while True:
            rows = cursor.fetchmany()
            if not rows:
                return
            yield from rows

    @staticmethod
    def concat_ids(ids: list) -> str:
        base_ids = ''
        for i in ids:
            base_ids += "'" + i[0] + "'" + ', '
        return base_ids[:-2]

    def execute(self, cursor, request: str, dataclass=None) -> List[Any]:
        cursor.execute(request)

        data = self.load(cursor)
        if not dataclass:
            return [elem for elem in data]
        else:
            return [dataclass(*elem) for elem in data]


class Genre(Extract):
    def __init__(self, base, old_state, start, next_):
        super().__init__(base, old_state, start, next_)
        self.dataclass = Genre_schema
        self.where_condition = f"WHERE g.modified > '{self.old_state['modified']}'"

    def producer(self, cursor) -> List[Any]:
        pass

    def enricher(self, cursor) -> List[Any]:
        pass

    def merger(self, cursor) -> List[Any]:
        return self.execute(cursor,
                            genre_merger(self.where_condition),
                            self.dataclass)
