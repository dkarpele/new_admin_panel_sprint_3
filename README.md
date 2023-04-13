# Запуск сервиса ETL

# docker-compose up

или вручную

# python listen.py

Сервис слушает изменения или добавления записей в таблицах `person`, `genre`,
`film_work`, после чего запускается класс ETL(), который загружает данные 
(у которых время обновления больше той, что в файле `state.json`) в 
elasticsearch. После загрузки пачки записей обновляется время загрузки 
последней записи в ES в файле `state.json`.
Пример дефолтного файла `state.json.example`.

Наполнения пустой базы происходит в `listen.py` на 73 строке. Так как по дефолту в
`state.json` установлена дата "1970-01-01 00:00:00.000000+00", то все фильмы у
которых время обновления больше той, что в файле `state.json` (а это вообще все фильмы),
будут добавлены в ES.