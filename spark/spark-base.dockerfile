FROM debian:12

RUN apt update
RUN apt install -y python3 python3-pip python3-venv curl wget tar procps

# Install Java 11 from AWS's Corretto distribution
RUN apt-get install -y java-common
RUN wget https://corretto.aws/downloads/latest/amazon-corretto-11-aarch64-linux-jdk.deb
RUN dpkg --install amazon-corretto-11-aarch64-linux-jdk.deb

ENV JAVA_HOME=/usr/lib/jvm/java-11-amazon-corretto

# Install Spark
RUN mkdir /opt/spark
RUN wget -O spark.tgz https://archive.apache.org/dist/spark/spark-3.5.3/spark-3.5.3-bin-hadoop3-scala2.13.tgz
RUN tar -xf spark.tgz -C /opt/spark --strip-components=1

# Install the iceberg package for spark
RUN wget -O /opt/spark/jars/iceberg-spark-runtime-3.5_2.13-1.7.0.jar https://search.maven.org/remotecontent?filepath=org/apache/iceberg/iceberg-spark-runtime-3.5_2.13/1.7.0/iceberg-spark-runtime-3.5_2.13-1.7.0.jar

# Install the hadoop-aws package for spark, to use S3 / minio
RUN wget -O /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar
RUN wget -O /opt/spark/jars/hadoop-aws-3.3.4.jar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar

# # Install the unitycatalog package for spark
# RUN wget -O /opt/spark/jars/unitycatalog-spark_2.13-0.2.0.jar https://repo1.maven.org/maven2/io/unitycatalog/unitycatalog-spark_2.13/0.2.0/unitycatalog-spark_2.13-0.2.0.jar


ENV SPARK_HOME=/opt/spark

