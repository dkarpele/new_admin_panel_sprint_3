import abc
import logging.config

from typing import List, Any

from schemas import MergerFilmWork, MergerPerson, MergerGenre
from config import DBCreds
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

    @db_cursor_backoff(DBCreds().dict(), db_type='pg')
    def extract(self, cursor, connection) -> dict[Any]:
        merger = self.merger(cursor)
        return merger

    @abc.abstractmethod
    def producer(self, cursor) -> List[Any]:
        pass

    @abc.abstractmethod
    def enricher(self, cursor) -> List[Any]:
        pass

    @abc.abstractmethod
    def merger(self, cursor) -> dict[Any]:
        pass

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
            base_ids += "'" + i.id_ + "'" + ', '
        return base_ids[:-2]

    def execute(self, cursor, request: str, dataclass=None) -> List[Any]:
        cursor.execute(request)

        data = self.load(cursor)
        if not dataclass:
            return [elem for elem in data]
        else:
            return [dataclass(*elem) for elem in data]

    def merger_filmwork(self, cursor):
        producer, enricher = self.enricher(cursor)
        if not enricher:
            return {}
        films_ids = self.concat_ids(enricher)
        where_condition = f"WHERE fw.id IN ({films_ids})"
        self.limit_condition = ''
        m = Filmwork(self.base,
                     self.old_state,
                     start=0,
                     next_=self.next_,
                     where_condition=where_condition,
                     limit_condition=self.limit_condition)
        return producer, m.merger(cursor)


class Person(Extract):
    def __init__(self, base, old_state, start, next_):
        super().__init__(base, old_state, start, next_)
        self.dataclass = MergerPerson

    def producer(self, cursor) -> List[Any]:
        where_condition = f"WHERE modified > '{self.old_state['modified']}'"

        return self.execute(cursor,
                            person_producer(where_condition,
                                            self.limit_condition),
                            dataclass=self.dataclass)

    def enricher(self, cursor) -> tuple:
        producer = self.producer(cursor)
        if not producer:
            return ()
        persons_ids = self.concat_ids(producer)
        where_condition = f"WHERE pfw.person_id IN ({persons_ids})"

        return producer, self.execute(cursor,
                                      person_enricher(where_condition),
                                      dataclass=MergerFilmWork)

    def merger(self, cursor) -> dict[Any]:
        """
        :param cursor:
        :return: Dict of es index as a key and data, {'persons': [data]}
        """
        res = {}
        persons, films = self.merger_filmwork(cursor)
        res.update(films)
        res['persons'] = persons
        return res


class Genre(Extract):
    def __init__(self, base, old_state, start, next_):
        super().__init__(base, old_state, start, next_)
        self.dataclass = MergerGenre

    def producer(self, cursor) -> List[Any]:
        where_condition = f"WHERE modified > '{self.old_state['modified']}'"
        return self.execute(cursor,
                            genre_producer(where_condition),
                            dataclass=self.dataclass)

    def enricher(self, cursor) -> tuple:
        producer = self.producer(cursor)
        if not producer:
            return ()
        genres_ids = self.concat_ids(producer)
        where_condition = f"WHERE gfw.genre_id IN ({genres_ids})"

        return producer, self.execute(cursor,
                                      genre_enricher(where_condition),
                                      dataclass=self.dataclass)

    def merger(self, cursor) -> dict[Any]:
        """
        :param cursor:
        :return: Dict of es index as a key and data, {'genres': [data]}
        """
        res = {}
        genres, films = self.merger_filmwork(cursor)
        res.update(films)
        res['genres'] = genres
        return res


class Filmwork(Extract):
    def __init__(self,
                 base,
                 old_state,
                 start,
                 next_,
                 where_condition,
                 limit_condition=''):
        super().__init__(base, old_state, start, next_)
        self.dataclass = MergerFilmWork
        self.where_condition = where_condition
        self.limit_condition = limit_condition

    def producer(self, cursor):
        pass

    def enricher(self, cursor):
        pass

    def merger(self, cursor) -> dict[Any]:
        return {'movies': self.execute(
                            cursor,
                            filmwork_merger(self.where_condition,
                                            self.limit_condition),
                            self.dataclass)
                }
