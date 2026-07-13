FROM apache/airflow:3.3.0

USER root

# 1. Instalar Java 17 e utilitários
RUN apt-get update && \
    apt-get install -y openjdk-17-jdk wget procps ant git && \
    apt-get clean;

# 2. Configurar Variáveis
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV SPARK_HOME=/opt/airflow/spark
ENV PATH=$PATH:$SPARK_HOME/bin

# 3. Instalar Spark 3.5.0
RUN wget -q https://archive.apache.org/dist/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz && \
    tar xzf spark-3.5.0-bin-hadoop3.tgz && \
    mv spark-3.5.0-bin-hadoop3 $SPARK_HOME && \
    rm spark-3.5.0-bin-hadoop3.tgz

# 4. DOWNLOAD DE JARS CRÍTICOS (Postgres + Integração com MinIO/S3)
# Postgres Driver
RUN wget -q https://jdbc.postgresql.org/download/postgresql-42.7.3.jar -O $SPARK_HOME/jars/postgresql-42.7.3.jar
# Hadoop AWS (Necessário para s3a://)
RUN wget -q https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar -O $SPARK_HOME/jars/hadoop-aws-3.3.4.jar
# AWS Java SDK Bundle (Necessário para s3a://)
RUN wget -q https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar -O $SPARK_HOME/jars/aws-java-sdk-bundle-1.12.262.jar

USER airflow

# 5. Instalar Libs Python (Incluindo dbt-postgres e minio)
RUN pip install --no-cache-dir \
    pyspark==3.5.0 \
    delta-spark==3.0.0 \
    pandas \
    requests \
    minio \
    dbt-core \
    dbt-postgres \
    clickhouse-connect \
    apache-airflow-providers-standard \
    apache-airflow-providers-fab