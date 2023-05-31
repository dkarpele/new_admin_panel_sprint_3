import logging.config

from state import JsonFileStorage, State
from config import STATE_FILE, CHUNK_SIZE
import extract
import load

logging.config.fileConfig(fname='logger.conf', disable_existing_loggers=False)

# Get the logger specified in the file
logger = logging.getLogger(__name__)


class ETL:
    def __init__(self, base: str, new_state: str):
        self.base = base
        self.old_state = State(JsonFileStorage(STATE_FILE)).get_state(self.base)
        self.new_state = new_state

        self.etl()

    def etl(self):
        start = 0
        next_ = CHUNK_SIZE

        while True:
            if self.base == 'film_work':
                where_condition = f"WHERE fw.modified > '{self.old_state['modified']}'"

                init_extract = extract.Filmwork(self.base,
                                                self.old_state,
                                                start,
                                                next_,
                                                where_condition)
            elif self.base == 'person':
                init_extract = extract.Person(self.base,
                                              self.old_state,
                                              start,
                                              next_)
            elif self.base == 'genre':
                init_extract = extract.Genre(self.base,
                                             self.old_state,
                                             start,
                                             next_)
            else:
                logging.error(f'Database {self.base} doesn\'t exists!')
                break

            data = init_extract.extract()
            if not data:
                break

            start += next_

            # Load data to ES
            es = load.Load()
            es.es_bulk_load(es.prepare_bulk_load(data))

            # Update state using latest modified time
            if self.base == 'film_work':
                try:
                    max_item = max([(item.modified, item.id_) for item in data])

                    new_state = {'id': str(max_item[1]),
                                 'modified': str(max_item[0])}
                    State(JsonFileStorage(STATE_FILE)).set_state(self.base,
                                                                 new_state)
                except IndexError:
                    pass
            else:
                State(JsonFileStorage(STATE_FILE)).set_state(self.base,
                                                             self.new_state)
