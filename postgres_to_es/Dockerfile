FROM python:3.10

ENV APP_HOME=/opt/etl

WORKDIR $APP_HOME

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

COPY ../requirements.txt requirements.txt

RUN  pip install --upgrade pip --no-cache-dir \
     && pip install -r requirements.txt --no-cache-dir

COPY . .

python listen.py/
