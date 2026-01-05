FROM apache/airflow:slim-3.1.5-python3.13
USER root
RUN apt update && \
    apt-get install -y openjdk-17-jdk && \
    apt-get install -y ant && \
    apt-get clean;

ENV JAVA_HOME /usr/lib/jvm/java-17-openjdk-amd64/
RUN export JAVA_HOME

USER airflow
COPY requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt
