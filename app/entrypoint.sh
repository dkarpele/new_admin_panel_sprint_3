#!/bin/sh

if [ "$DATABASE" = "postgres" ]
then
    echo "Waiting for postgres on $DB_HOST:$DB_PORT ..."

    while ! nc -z "$DB_HOST" "$DB_PORT"; do
      sleep 0.1
    done

    echo "PostgreSQL started"
fi

python manage.py migrate
uwsgi --strict --ini uwsgi.ini
exec "$@"