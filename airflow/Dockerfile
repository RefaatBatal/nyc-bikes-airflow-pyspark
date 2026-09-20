FROM apache/airflow:3.3.1

USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jdk-headless && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

USER airflow
RUN pip install --no-cache-dir pyspark==4.2.0 elasticsearch==8.15.0
RUN pip install --no-cache-dir great_expectations==1.23.0