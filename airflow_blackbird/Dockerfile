FROM apache/airflow:2.3.4-python3.8

USER root
RUN sudo apt-get update
RUN sudo apt-get install -y --no-install-recommends \
         vim gcc git unzip psmisc direnv pkg-config\
         default-libmysqlclient-dev libpq-dev libaio1

RUN mkdir -p /opt/settings
COPY ./settings /opt/settings
USER airflow

RUN python -m pip install --upgrade pip
RUN pip install "apache-airflow[celery, postgres, redis, crypto, statsd, elasticsearc, google]==2.3.4" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.3.4/constraints-3.8.txt"
