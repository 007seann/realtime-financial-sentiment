

# Use the official Airflow 2.8.1 image with Python 3.8 as the base
FROM apache/airflow:2.8.1-python3.8

# # Set Airflow home directory
ENV AIRFLOW_HOME=/opt/airflow
# ENV AIRFLOW__CORE__EXECUTOR=SequentialExecutor

# Switch to root for system-level installations
USER root

RUN apt-get update \
    && apt-get install -y \
        wget \
        curl \
        supervisor \
        postgresql-client \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* 


# Install Java 21 (Temurin from Adoptium)
RUN wget https://github.com/adoptium/temurin21-binaries/releases/download/jdk-21.0.2%2B13/OpenJDK21U-jdk_x64_linux_hotspot_21.0.2_13.tar.gz \
&& mkdir -p /opt/java/openjdk-21 \
&& tar -xzf OpenJDK21U-jdk_x64_linux_hotspot_21.0.2_13.tar.gz -C /opt/java/openjdk-21 --strip-components=1 \
&& rm OpenJDK21U-jdk_x64_linux_hotspot_21.0.2_13.tar.gz

# Set JAVA_HOME and update PATH
ENV JAVA_HOME=/opt/java/openjdk-21
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Install Spark 3.2.1
ENV SPARK_VERSION=3.5.4
ENV SPARK_HADOOP_VERSION=3
RUN wget -q https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${SPARK_HADOOP_VERSION}.tgz \
    && tar -xzf spark-${SPARK_VERSION}-bin-hadoop${SPARK_HADOOP_VERSION}.tgz -C /opt/ \
    && rm spark-${SPARK_VERSION}-bin-hadoop${SPARK_HADOOP_VERSION}.tgz

# Set Spark environment variables
ENV SPARK_HOME=/opt/spark-${SPARK_VERSION}-bin-hadoop${SPARK_HADOOP_VERSION}
ENV PATH=$PATH:${SPARK_HOME}/bin:${SPARK_HOME}/sbin

# Copy custom JAR files for Spark
# COPY kafka-clients-2.8.0.jar $SPARK_HOME/jars/
# COPY postgresql-42.2.20.jar $SPARK_HOME/jars/

RUN chown -R airflow:root $SPARK_HOME
# Switch to airflow user for Python installations
USER airflow

# Ensure .local/bin is in PATH for user-installed packages
ENV PATH="/home/airflow/.local/bin:$PATH"

# Install Python dependencies
WORKDIR /opt/airflow
COPY requirements.txt .

COPY deps /deps/

RUN pip install --user psycopg2-binary
RUN pip install --user --no-index --find-links=/deps httpx==0.25.2
RUN pip install --user --no-index --find-links=/deps psycopg2-binary pyspark==3.2.1 \
&& if [ -f requirements.txt ]; then pip install --user --no-index --find-links=/deps -r requirements.txt; fi \
&& rm -rf /home/airflow/.local/lib/python3.8/site-packages/airflow/example_dags \
&& rm -rf /home/airflow/.cache/pip

# RUN pip install --user psycopg2-binary pyspark==3.2.1 \
#     && if [ -f requirements.txt ]; then pip install --user -r requirements.txt; fi \
#     && rm -rf /home/airflow/.local/lib/python3.8/site-packages/airflow/example_dags \
#     && rm -rf /home/airflow/.cache/pip

# Switch back to root to copy config files and set permissions
USER root

# Copy supervisor and entrypoint configuration
COPY supervisord.conf /etc/supervisor/conf.d/supervisord.conf
COPY entrypoint.sh /opt/airflow/entrypoint.sh
RUN chmod +x /opt/airflow/entrypoint.sh

# Set permissions for Airflow directories
RUN mkdir -p /opt/airflow/logs && chown -R airflow:$(id -gn airflow) /opt/airflow/logs

# Expose ports (optional, controlled by docker-compose.yml)
EXPOSE 22 3000 4040 8000

# Switch to airflow user for runtime
USER airflow




