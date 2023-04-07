import extract

from state import JsonFileStorage, State
from schemas import STATE_FILE


class Transform:
    def __init__(self, key: str, state: str):
        self.key = key
        self.old_state = State(JsonFileStorage(STATE_FILE)).get_state(self.key)
        self.new_state = state
        if self.key == 'person':
            self.transform_person()
        elif self.key == 'genre':
            self.transform_genre()
        elif self.key == 'film_work':
            self.transform_film_work()

    def transform_person(self) -> str:
        """ Transform data from database format to es format """
        person = extract.Person(self.key, self.old_state, self.new_state)
        person.extract()

    def transform_genre(self):
        pass

    def transform_film_work(self):
        filmwork = extract.Filmwork(self.key, self.old_state, self.new_state)
        ext = filmwork.extract()
        State(JsonFileStorage(STATE_FILE)).set_state(self.key, self.new_state)
        print(ext)


    
    
