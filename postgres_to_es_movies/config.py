import json
import logging.config
import os

from pydantic import BaseSettings, Field

from dotenv import load_dotenv

from schemas import FilmWork, Person, Genre, GenreFilmWork, PersonFilmWork

logging.config.fileConfig(fname='logger.conf', disable_existing_loggers=False)

# Get the logger specified in the file
logger = logging.getLogger(__name__)

load_dotenv()

SCHEMA = os.environ.get('PG_SCHEMA')
TRIGGER = 'modified'
CHUNK_SIZE = 100


class DBCreds(BaseSettings):
    dbname: str = Field(..., env="DB_NAME")
    user: str = Field(..., env="DB_USER")
    password: str = Field(..., env="DB_PASSWORD")
    host: str = Field(env="DB_HOST", default='127.0.0.1')
    port: int = Field(env="DB_PORT", default=5432)
    options: str = '-c search_path=%s' % os.environ.get('PG_SCHEMA')

    class Config:
        env_prefix = ""
        case_sentive = False
        env_file = '.env'
        env_file_encoding = 'utf-8'


# Don't change the database order!
DATABASE_LIST = {
    'film_work': FilmWork,
    'person': Person,
    'genre': Genre,
    'person_film_work': PersonFilmWork,
    'genre_film_work': GenreFilmWork,
}


ES_SCHEMA_FILES = 'es_schema_movies es_schema_persons es_schema_genres'


class ESCreds(BaseSettings):
    hosts: str = Field(env="ES_HOST", default='127.0.0.1:9200')

    class Config:
        env_prefix = ""
        case_sentive = False
        env_file = '.env'
        env_file_encoding = 'utf-8'


def es_settings_mappings(index: str):
    settings, mappings, doc = {}, {}, {}
    file_name = ''
    for i in ES_SCHEMA_FILES.split():
        if index in i:
            file_name = i
            break

    with open(file_name, 'r') as openfile:
        try:
            schema = json.load(openfile)
            settings = schema['settings']
            mappings = schema['mappings']
        except IOError as err:
            logging.error(err)
    for keys in mappings['properties'].keys():
        doc.update({keys: ''})
    return settings, mappings, doc


START_SLEEP_TIME = 1
FACTOR = 2
BORDER_SLEEP_TIME = 10

STATE_FILE = 'state.json'
