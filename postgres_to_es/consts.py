import json
import os

from dotenv import load_dotenv

from schemas import FilmWork, Person, Genre, GenreFilmWork, PersonFilmWork

load_dotenv()

SCHEMA = 'content'
TRIGGER = 'modified'
CHUNK_SIZE = 5
DSL = {'dbname': os.environ.get('DB_NAME'),
       'user': os.environ.get('DB_USER'),
       'password': os.environ.get('DB_PASSWORD'),
       'host': os.environ.get('DB_HOST', '127.0.0.1'),
       'port': os.environ.get('DB_PORT', 5432),
       'options': '-c search_path=%s' % os.environ.get('SEARCH_PATH')}

STATE_FILE = 'state.json'

ES_SCHEMA_FILE = 'es_schema'
ES_INDEX = os.environ.get('ES_INDEX', 'movies')
ES = {'hosts': os.environ.get('ES_HOST', '127.0.0.1:9200')}


def es_settings_mappings():
    settings, mappings, doc = {}, {}, {}
    with open(ES_SCHEMA_FILE, 'r') as openfile:
        try:
            schema = json.load(openfile)
            settings = schema['settings']
            mappings = schema['mappings']
        except IOError as err:
            print(err)
    for keys in mappings['properties'].keys():
        doc.update({keys: ''})
    return settings, mappings, doc


ES_SETTINGS = es_settings_mappings()[0]
ES_MAPPINGS = es_settings_mappings()[1]
ES_DOC = es_settings_mappings()[2]

# Don't change the database order!
DATABASE_LIST = {
    'film_work': FilmWork,
    'person': Person,
    'genre': Genre,
    'person_film_work': PersonFilmWork,
    'genre_film_work': GenreFilmWork,
}

START_SLEEP_TIME = 1
FACTOR = 2
BORDER_SLEEP_TIME = 10
