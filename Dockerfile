FROM apache/airflow:2.8.4

USER root
RUN apt-get update && apt-get install -y openjdk-17-jdk curl ca-certificates git && apt-get clean

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

USER airflow
RUN pip install --no-cache-dir apache-airflow==2.8.4 apache-airflow-providers-apache-spark pyspark==3.4.4

ENV PATH=/home/airflow/.local/bin:$PATH