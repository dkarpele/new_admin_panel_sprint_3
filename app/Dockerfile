FROM python:3.10

ENV APP_HOME=/opt/app

WORKDIR $APP_HOME

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1
ENV UWSGI_PROCESSES 1
ENV UWSGI_THREADS 16
ENV UWSGI_HARAKIRI 240
ENV DJANGO_SETTINGS_MODULE 'config.settings'

COPY ../requirements.txt requirements.txt
COPY entrypoint.sh entrypoint.sh
COPY uwsgi/uwsgi.ini uwsgi.ini

RUN  mkdir -p /var/www/static/ \
     && mkdir -p /var/www/media/ \
     && mkdir -p $APP_HOME/static/ \
     && mkdir -p $APP_HOME/staticfiles/ \
     && mkdir -p $APP_HOME/media/ \
     && sed -i 's/\r$//g' $APP_HOME/entrypoint.sh \
     && chmod +x $APP_HOME/entrypoint.sh \
     && pip install --upgrade pip --no-cache-dir \
     && pip install -r requirements.txt --no-cache-dir \
     && apt-get update && apt-get install -y netcat

COPY . .

EXPOSE 8000
