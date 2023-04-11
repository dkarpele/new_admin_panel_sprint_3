import abc
import logging.config

from typing import List, Any

from schemas import Merger
from consts import DSL
from pg_sql_request import filmwork_merger, person_producer, person_enricher, \
    genre_producer, genre_enricher
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
        self.limit_condition =\
            f"""OFFSET {str(self.start)} ROWS
                LIMIT {str(self.next_)} """

    @db_cursor_backoff(DSL, db_type='db')
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
        enricher = self.enricher(cursor)
        if not enricher:
            return []
        films_ids = self.concat_ids(enricher)
        where_condition = f"WHERE fw.id IN ({films_ids})"

        m = Filmwork(self.base,
                     self.old_state,
                     self.start,
                     self.next_,
                     where_condition)
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


class Person(Extract):
    def __init__(self, base, old_state, start, next_):
        super().__init__(base, old_state, start, next_)

    def producer(self, cursor) -> List[Any]:
        where_condition = f"WHERE modified > '{self.old_state['modified']}'"

        return self.execute(cursor,
                            person_producer(where_condition,
                                            self.limit_condition))

    def enricher(self, cursor) -> List[Any]:
        producer = self.producer(cursor)
        if not producer:
            return []
        persons_ids = self.concat_ids(producer)
        where_condition = f"WHERE pfw.person_id IN ({persons_ids})"

        return self.execute(cursor,
                            person_enricher(where_condition,
                                            self.limit_condition))


class Genre(Extract):
    def __init__(self, base, old_state, start, next_):
        super().__init__(base, old_state, start, next_)

    def producer(self, cursor) -> List[Any]:
        where_condition = f"WHERE modified > '{self.old_state['modified']}'"
        return self.execute(cursor, genre_producer(where_condition))

    def enricher(self, cursor) -> List[Any]:
        producer = self.producer(cursor)
        if not producer:
            return []
        genres_ids = self.concat_ids(producer)
        where_condition = f"WHERE gfw.genre_id IN ({genres_ids})"

        return self.execute(cursor,
                            genre_enricher(where_condition,
                                           self.limit_condition))


class Filmwork(Extract):
    def __init__(self,
                 base,
                 old_state,
                 start,
                 next_,
                 where_condition):
        super().__init__(base, old_state, start, next_)
        self.dataclass = Merger
        self.where_condition = where_condition

    def producer(self, cursor):
        pass

    def enricher(self, cursor):
        pass

    def merger(self, cursor) -> List[Any]:
        return self.execute(cursor,
                            filmwork_merger(self.where_condition,
                                            self.limit_condition),
                            self.dataclass)
