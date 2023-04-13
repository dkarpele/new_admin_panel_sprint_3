#!/bin/sh

if [ "$DATABASE" = "postgres" ]
then
    echo "Waiting for postgres..."

    while ! nc -z "$DB_HOST" "$DB_PORT"; do
      sleep 0.1
    done

    echo "PostgreSQL started"
fi

if [ "$ES_DATABASE" = "es" ]
then
    echo "Waiting for es..."

    while ! nc -z "$DB_HOST" "9200"; do
      sleep 0.1
    done

    echo "ES started"
fi

python listen.py
exec "$@"