import elasticsearch.exceptions
import elasticsearch
import logging.config
import os

from dotenv import load_dotenv

from config import ESCreds, ES_SETTINGS, ES_MAPPINGS, ES_INDEX_MOVIES
from tools import db_cursor_backoff
logging.config.fileConfig(fname='logger.conf', disable_existing_loggers=False)

# Get the logger specified in the file
logger = logging.getLogger(__name__)

load_dotenv()

_index = os.environ.get('ES_INDEX_MOVIES', 'movies')
es_file_path = 'es_schema'


class Load:
    def __init__(self):
        self._index = _index
        self.file_path = es_file_path

    @db_cursor_backoff(ESCreds().dict(), db_type='es')
    def es_create_index_if_not_exists(self, connection):
        """Create the given ElasticSearch index and ignore error if it already
        exists"""
        try:
            connection.indices.create(index=self._index,
                                      settings=ES_SETTINGS,
                                      mappings=ES_MAPPINGS)
        except elasticsearch.exceptions.RequestError as ex:
            if ex.error == 'resource_already_exists_exception':
                logging.info('Index already exists. Ignore.')

    @staticmethod
    def prepare_bulk_load(data) -> list:
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
        for merger in data:

            action = {"index": {"_index": ES_INDEX_MOVIES,
                                "_id": merger.id_}}

            doc = {"id": merger.id_,
                   "imdb_rating": merger.imdb_rating,
                   "genre": actors_writers_directors_genre(merger.genre),
                   "title": merger.title,
                   "description": merger.description,
                   "directors": actors_writers_directors_genre(merger.directors),
                   "actors_names": persons_list(merger.actors),
                   "writers_names": persons_list(merger.writers),
                   "actors": actors_writers_directors_genre(merger.actors),
                   "writers": actors_writers_directors_genre(merger.writers)}

            operation.append(action)
            operation.append(doc)

        return operation

    @db_cursor_backoff(ESCreds().dict(), db_type='es')
    def es_bulk_load(self, connection, operation: list):
        res = connection.bulk(index=ES_INDEX_MOVIES,
                              operations=operation,
                              filter_path="items.*.error",
                              source=True)
        uploaded_ids = [index['index']['_id']
                        for n, index in enumerate(operation) if n % 2 != 1]
        if not res.body:
            logging.info(f'Docs uploaded successfully with id_ below: \n'
                         f'{uploaded_ids}')

        else:
            logging.error(f'{res.body["items"]} failed to upload to ES.')
