import uuid
from datetime import datetime, date
from dataclasses import dataclass, field
from dotenv import load_dotenv

load_dotenv()


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
class Genre(MixinDate, MixinId):
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


@dataclass
class Merger(MixinDate, MixinId):
    title: str = field(default="")
    description: str = field(default="")
    imdb_rating: float = field(default=0.0)
    genre: list = field(default=list)
    actors: list = field(default=list)
    writers: list = field(default=list)
    directors: list = field(default=list)

