import abc
from typing import Any
import json


class BaseStorage(abc.ABC):
    """Абстрактное хранилище состояния.

    Позволяет сохранять и получать состояние.
    Способ хранения состояния может варьироваться в зависимости
    от итоговой реализации. Например, можно хранить информацию
    в базе данных или в распределённом файловом хранилище.
    """

    @abc.abstractmethod
    def save_state(self, state: dict) -> None:
        """Сохранить состояние в хранилище."""

    @abc.abstractmethod
    def retrieve_state(self) -> dict[str, Any]:
        """Получить состояние из хранилища."""


class JsonFileStorage(BaseStorage):
    """Реализация хранилища, использующего локальный файл.

    Формат хранения: JSON
    """

    def __init__(self, file_path: str) -> None:
        self.file_path = file_path

    def save_state(self, state: dict[str, Any]) -> None:
        """Сохранить состояние в хранилище."""
        try:
            json_object = json.dumps(state, indent=4)
            with open(self.file_path, "w") as outfile:
                outfile.write(json_object)
        except IOError:
            return None

    def retrieve_state(self) -> dict[str, Any]:
        """Получить состояние из хранилища."""
        state_dict = {}
        try:
            with open(self.file_path, 'r') as openfile:
                state_dict: dict[str, Any] = json.load(openfile)
        except IOError as err:
            print(err)
        if not self.file_path:
            return state_dict
        if not state_dict:
            return {}
        return state_dict


class State:
    """Класс для работы с состояниями."""

    def __init__(self, storage: BaseStorage) -> None:
        self.storage = storage

    def set_state(self, key: str, value: Any) -> None:
        """Установить состояние для определённого ключа."""
        state_dict = self.storage.retrieve_state()
        state_dict.update({key: value})
        self.storage.save_state(state_dict)

    def get_state(self, key: str) -> Any:
        """Получить состояние по определённому ключу."""
        try:
            if not self.storage.retrieve_state()[key]:
                return None
            elif self.storage.retrieve_state()[key] == '':
                return None
            else:
                return self.storage.retrieve_state()[key]
        except KeyError:
            return None

#
# state_dict = {
#     'film_work': {
#         'modified': '1970-01-01 00:00:00'
#     },
#     'person': {
#         'modified': '1970-01-01 00:00:00'
#     },
#     'genre': {
#         'modified': '1970-01-01 00:00:00'
#     },
# }
# def create_json_file(file_name):
#     state = {'modified': '23:23:23'}
#     with open(file_name, "w") as outfile:
#         json.dump(state, outfile)
#
#
# def main_method():
#     file_path = "state"
#     key = 'modified'
#     value = '11:11:11'
#     # create_json_file(file_path)
#     stor = JsonFileStorage(file_path)
#     state = State(stor)
#     state.get_state(key)
#     state.set_state(key, value)
#     print(state.get_state(key))
#
#
# if __name__ == '__main__':
#     main_method()
