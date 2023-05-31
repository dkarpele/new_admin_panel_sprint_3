#!/bin/sh

if [ "$DATABASE" = "postgres" ]
then
    echo "Waiting for postgres on $DB_HOST:$DB_PORT ..."

    while ! nc -z "$DB_HOST" "$DB_PORT"; do
      sleep 0.1
    done

    echo "PostgreSQL started"
fi

ES_HOSTNAME=$(echo "$ES_HOST" | cut -d":" -f2  | cut -d"/" -f3)
ES_PORT=$(echo "$ES_HOST" | cut -d":" -f3)

if [ "$ES_DATABASE" = "es" ]
then
    echo "Waiting for es on $ES_HOSTNAME:$ES_PORT..."

    while ! nc -z "$ES_HOSTNAME" "$ES_PORT"; do
      sleep 0.1
    done

    echo "ES started"
fi

python listen.py
exec "$@"