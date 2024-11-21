FROM debian:12

RUN apt update
RUN apt install -y python3 python3-pip curl wget tar procps

# Install Java 11 from AWS's Corretto distribution
RUN apt-get install -y java-common
RUN wget https://corretto.aws/downloads/latest/amazon-corretto-11-aarch64-linux-jdk.deb
RUN dpkg --install amazon-corretto-11-aarch64-linux-jdk.deb

ENV JAVA_HOME=/usr/lib/jvm/java-11-amazon-corretto


# Install Spark
ENV SPARK_VERSION=3.5.0
ENV HADOOP_VERSION=3

RUN mkdir /opt/spark
RUN wget -O spark.tgz https://archive.apache.org/dist/spark/spark-3.5.3/spark-3.5.3-bin-hadoop3.tgz
RUN tar -xf spark.tgz -C /opt/spark --strip-components=1

ENV SPARK_HOME=/opt/spark

