import elasticsearch.exceptions
import elasticsearch
import logging.config
import os

from dotenv import load_dotenv

from consts import ES, ES_SETTINGS, ES_MAPPINGS, ES_INDEX
from tools import db_cursor_backoff
logging.config.fileConfig(fname='logger.conf', disable_existing_loggers=False)

# Get the logger specified in the file
logger = logging.getLogger(__name__)

load_dotenv()

_index = os.environ.get('ES_INDEX', 'movies')
es_file_path = 'es_schema'


class Load:
    def __init__(self):
        self._index = _index
        self.file_path = es_file_path

    @db_cursor_backoff(ES, db_type='es')
    def es_create_index_if_not_exists(self):
        """Create the given ElasticSearch index and ignore error if it already
        exists"""
        es = elasticsearch.Elasticsearch(**ES)
        try:
            es.indices.create(index=self._index,
                              settings=ES_SETTINGS,
                              mappings=ES_MAPPINGS)
        except elasticsearch.exceptions.RequestError as ex:
            if ex.error == 'resource_already_exists_exception':
                logging.info('Index already exists. Ignore.')

    @staticmethod
    def prepare_bulk_load(data) -> list:
        def actors_writers(persons):
            id_name = []
            try:
                for i in persons:
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

            action = {"index": {"_index": ES_INDEX,
                                "_id": merger.id_}}

            doc = {"id": merger.id_,
                   "imdb_rating": merger.imdb_rating,
                   "genre": merger.genre,
                   "title": merger.title,
                   "description": merger.description,
                   "director": persons_list(merger.directors),
                   "actors_names": persons_list(merger.actors),
                   "writers_names": persons_list(merger.writers),
                   "actors": actors_writers(merger.actors),
                   "writers": actors_writers(merger.writers)}

            operation.append(action)
            operation.append(doc)

        return operation

    @db_cursor_backoff(ES, db_type='es')
    def es_bulk_load(self, operation: list):
        es = elasticsearch.Elasticsearch(**ES)
        res = es.bulk(index=ES_INDEX,
                      operations=operation,
                      filter_path="items.*.error")
        if not res.body:
            logging.info('Docs uploaded successfully.')
        else:
            logging.error(f'{res.body["items"]} failed to upload to ES.')
