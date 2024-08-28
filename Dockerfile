FROM apache/airflow:2.9.3


ENV AIRFLOW_HOME=/opt/airflow

WORKDIR $AIRFLOW_HOME

USER root
RUN apt-get update -qq && apt-get install vim -qqq && apt-get install -y python3-pip  && apt-get install -y gosu  && apt-get install -y wget

ENV JAVA_HOME=/home/jdk-11.0.2
ENV PATH="${JAVA_HOME}/bin/:${PATH}"
RUN export JAVA_HOME

# Install OpenJDK-11
RUN DOWNLOAD_URL="https://download.java.net/java/GA/jdk11/9/GPL/openjdk-11.0.2_linux-x64_bin.tar.gz" \
    && TMP_DIR="$(mktemp -d)" \
    && curl -fL "${DOWNLOAD_URL}" --output "${TMP_DIR}/openjdk-11.0.2_linux-x64_bin.tar.gz" \
    && mkdir -p "${JAVA_HOME}" \
    && tar xzf "${TMP_DIR}/openjdk-11.0.2_linux-x64_bin.tar.gz" -C "${JAVA_HOME}" --strip-components=1 \
    && rm -rf "${TMP_DIR}" \
    && java --version
    

# Install Spark
ENV SPARK_VERSION=3.4.3
ENV HADOOP_VERSION=3
RUN wget https://downloads.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    tar -xvzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark && \
    rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz    

# Set Spark environment variables
ENV SPARK_HOME /opt/spark
ENV PATH $PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin

COPY requirements.txt .

RUN python3 -m pip install --upgrade pip

RUN python3 -m pip install --no-cache-dir -r requirements.txt

USER $AIRFLOW_UID