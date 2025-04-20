FROM deltaio/delta-docker:latest

WORKDIR /opt/spark-apps

COPY ./app /opt/spark-apps/app
COPY ./data /opt/spark-apps/data

ARG DELTA_PACKAGE_VERSION=2.1.0

RUN curl -o /opt/spark/jars/hadoop-aws-3.2.0.jar https://repo.maven.apache.org/maven2/org/apache/hadoop/hadoop-aws/3.2.0/hadoop-aws-3.2.0.jar
RUN curl -o /opt/spark/jars/aws-java-sdk-bundle-1.11.901.jar https://repo.maven.apache.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.901/aws-java-sdk-bundle-1.11.901.jar

COPY config/core-site.xml /opt/spark/conf/
COPY config/hdfs-site.xml /opt/spark/conf/

EXPOSE 8080 7077

CMD ["spark-submit", \
     "--packages", "io.delta:delta-core_2.12:${DELTA_PACKAGE_VERSION}", \
     "--conf", "spark.driver.extraJavaOptions=-Divy.cache.dir=/tmp -Divy.home=/tmp", \
     "--conf", "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension", \
     "--conf", "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog", \
     "--conf", "spark.hadoop.fs.s3a.endpoint=http://minio:9000", \
     "--conf", "spark.hadoop.fs.s3a.access.key=minio", \
     "--conf", "spark.hadoop.fs.s3a.secret.key=minio123", \
     "--conf", "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem", \
     "--conf", "spark.hadoop.fs.s3a.path.style.access=true", \
     "/opt/spark-apps/app/main.py"]
