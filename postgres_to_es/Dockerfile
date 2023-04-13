FROM python:3.10

ENV APP_HOME=/opt/etl

WORKDIR $APP_HOME

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

COPY requirements.txt requirements.txt
COPY entrypoint.sh entrypoint.sh

RUN  sed -i 's/\r$//g' $APP_HOME/entrypoint.sh \
     && chmod +x $APP_HOME/entrypoint.sh \
     && pip install --upgrade pip --no-cache-dir \
     && pip install -r requirements.txt --no-cache-dir \
     && apt-get update && apt-get install -y netcat

COPY . .