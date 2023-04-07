import extract

from state import JsonFileStorage, State
from schemas import STATE_FILE


class Load:
    def __init__(self, key: str, state: str):
        self.key = key
        self.old_state = State(JsonFileStorage(STATE_FILE)).get_state(self.key)
        self.new_state = state
        if self.key == 'person':
            self.load_person()
        elif self.key == 'genre':
            self.load_genre()
        elif self.key == 'film_work':
            self.load_film_work()

    def load_person(self) -> str:
        """ Load data from database format to es format """
        person = extract.Person(self.key, self.old_state, self.new_state)
        person.extract()

    def load_genre(self):
        pass

    def load_film_work(self):
        filmwork = extract.Filmwork(self.key, self.old_state, self.new_state)
        ext = filmwork.extract()
        State(JsonFileStorage(STATE_FILE)).set_state(self.key, self.new_state)
        print(ext)


