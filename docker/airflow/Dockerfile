#this dockerfile is create for airflow
#docker/airflow/Dockerfile
FROM apache/airflow:2.10.0

USER root

# Installer Java 17 + outils nécessaires
RUN apt-get update && apt-get install -y \
    openjdk-17-jdk \
    curl \
    gnupg \
    && rm -rf /var/lib/apt/lists/*

# Installer sbt
RUN curl -fsSL https://repo.scala-sbt.org/scalasbt/debian/sbt-1.9.9.deb -o sbt.deb \
    && apt-get update \
    && apt-get install -y ./sbt.deb \
    && rm sbt.deb

USER airflow