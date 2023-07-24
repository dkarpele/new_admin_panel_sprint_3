import elasticsearch.exceptions
import elasticsearch
import logging.config
import os

from dotenv import load_dotenv

from config import ESCreds, es_settings_mappings
from tools import db_cursor_backoff
logging.config.fileConfig(fname='logger.conf', disable_existing_loggers=False)

# Get the logger specified in the file
logger = logging.getLogger(__name__)

load_dotenv()

_indexes = os.environ.get('ES_INDEXES', 'movies persons genres')
es_file_pathes = {'movies': 'es_schema_movies',
                  'persons': 'es_schema_persons',
                  'genres': 'es_schema_genres'}


class Load:
    def __init__(self):
        self._indexes = _indexes
        # self.file_path = es_file_path

    @db_cursor_backoff(ESCreds().dict(), db_type='es')
    def es_create_index_if_not_exists(self, connection):
        """Create the given ElasticSearch index and ignore error if it already
        exists"""
        for index in self._indexes.split():
            try:
                es_schema = es_settings_mappings(index)
                connection.indices.create(
                    index=index,
                    settings=es_schema[0],
                    mappings=es_schema[1])
            except elasticsearch.exceptions.RequestError as ex:
                if ex.error == 'resource_already_exists_exception':
                    logging.info(f'Index {index} already exists. Ignore.')

    def prepare_bulk_load(self, data) -> list:
        def actors_writers_directors_genre(items):
            id_name = []
            try:
                for i in items:
                    id_name.append(
                        {'id': i[: i.index(':')], 'name': i[i.index(':')+1:]})
                return id_name
            except TypeError:
                return []

        def persons_list(persons):
            try:
                return [i[i.index(':')+1:] for i in persons]
            except TypeError:
                return []

        operation = []
        for index, values in data.items():
            for merger in values:
                action = {"index": {"_index": index,
                                    "_id": merger.id_}}

                if index == 'movies':
                    doc = {
                       "id": merger.id_,
                       "imdb_rating": merger.imdb_rating,
                       "genre": actors_writers_directors_genre(merger.genre),
                       "title": merger.title,
                       "description": merger.description,
                       "directors": actors_writers_directors_genre(merger.directors),
                       "actors_names": persons_list(merger.actors),
                       "writers_names": persons_list(merger.writers),
                       "actors": actors_writers_directors_genre(merger.actors),
                       "writers": actors_writers_directors_genre(merger.writers)
                    }

                elif index == 'persons':
                    doc = {
                       "id": merger.id_,
                       "full_name": merger.full_name
                    }
                elif index == 'genres':
                    doc = {
                       "id": merger.id_,
                       "name": merger.name
                    }
                else:
                    raise Exception(f'only indexes {self._indexes} can be '
                                    f'used')
                operation.append(action)
                operation.append(doc)

        return operation

    @db_cursor_backoff(ESCreds().dict(), db_type='es')
    def es_bulk_load(self, connection, operation: list):
        for index in self._indexes.split():
            res = connection.bulk(index=index,
                                  operations=operation,
                                  filter_path="items.*.error",
                                  source=True)
            if res.body:
                logging.error(f'{res.body["items"]} failed to upload to ES.')
