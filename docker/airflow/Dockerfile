FROM apache/airflow:slim-latest-python3.8

USER root

RUN apt-get update

RUN apt-get install -y openjdk-17-jdk && \
    apt-get install -y ant && \
    apt-get clean

# Set JAVA_HOME environment variable
ENV JAVA_HOME /usr/lib/jvm/java-17-openjdk-arm64

RUN export JAVA_HOME

USER airflow

RUN pip install --no-cache-dir apache-airflow apache-airflow-providers-apache-spark pyspark psycopg2-binary 
