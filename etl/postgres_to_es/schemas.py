import os
import uuid
from datetime import datetime, date
from dataclasses import dataclass, field
from dotenv import load_dotenv

load_dotenv()

SCHEMA = 'content'
TRIGGER = 'modified'
STATE_FILE = 'state.json'
CHUNK_SIZE = 100

DSL = {'dbname': os.environ.get('DB_NAME'),
       'user': os.environ.get('DB_USER'),
       'password': os.environ.get('DB_PASSWORD'),
       'host': os.environ.get('DB_HOST', '127.0.0.1'),
       'port': os.environ.get('DB_PORT', 5432),
       'options': '-c search_path=%s' % os.environ.get('SEARCH_PATH')}

@dataclass
class MixinId:
    id_: uuid.UUID = field(default_factory=uuid.uuid4)


@dataclass
class MixinDate:
    created: datetime = datetime.now()
    modified: datetime = datetime.now()


@dataclass
class FilmWork(MixinId, MixinDate):
    title: str = field(default="")
    creation_date: date = datetime.now()
    description: str = field(default="")
    rating: float = field(default=0.0)
    type_: str = field(default="")


@dataclass
class Person(MixinId, MixinDate):
    full_name: str = field(default="")


@dataclass
class Genre(MixinId, MixinDate):
    name: str = field(default="")
    description: str = field(default="")


@dataclass
class GenreFilmWork(MixinId):
    genre_id: uuid.UUID = field(default_factory=uuid.uuid4)
    film_work_id: uuid.UUID = field(default_factory=uuid.uuid4)
    created: datetime = datetime.now()


@dataclass
class PersonFilmWork(MixinId):
    person_id: uuid.UUID = field(default_factory=uuid.uuid4)
    film_work_id: uuid.UUID = field(default_factory=uuid.uuid4)
    role: str = field(default="")
    created: datetime = datetime.now()


# Don't change the database order!
DATABASE_LIST = {
    'film_work': FilmWork,
    'person': Person,
    'genre': Genre,
    'person_film_work': PersonFilmWork,
    'genre_film_work': GenreFilmWork,
}
