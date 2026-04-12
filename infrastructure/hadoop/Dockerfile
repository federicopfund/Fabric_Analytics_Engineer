# ============================================================
# Hadoop 3.3.4 + Java 11 — Lakehouse Base Image
# ============================================================
FROM eclipse-temurin:11-jre-focal

LABEL maintainer="Federico Pfund <federicopfund@gmail.com>" \
      description="Hadoop 3.3.4 Lakehouse node with HDFS, YARN, and Hive support"

# Instalar dependencias del sistema
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    ssh \
    rsync \
    procps \
    net-tools \
    && rm -rf /var/lib/apt/lists/*

# Variables de entorno
ENV HADOOP_VERSION=3.3.4
ENV HADOOP_HOME=/opt/hadoop
ENV HADOOP_CONF_DIR=${HADOOP_HOME}/etc/hadoop
ENV PATH=${PATH}:${HADOOP_HOME}/bin:${HADOOP_HOME}/sbin
ENV JAVA_HOME=/opt/java/openjdk
ENV HDFS_NAMENODE_USER=root
ENV HDFS_DATANODE_USER=root
ENV HDFS_SECONDARYNAMENODE_USER=root

# Descargar e instalar Hadoop
RUN curl -fsSL https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz \
    | tar -xz -C /opt/ \
    && mv /opt/hadoop-${HADOOP_VERSION} ${HADOOP_HOME}

# Configurar SSH sin password (necesario para start-dfs.sh)
RUN ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa \
    && cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys \
    && chmod 0600 ~/.ssh/authorized_keys

# Copiar configuraciones de Hadoop (HDFS only — YARN no se usa)
COPY conf/core-site.xml ${HADOOP_CONF_DIR}/core-site.xml
COPY conf/hdfs-site.xml ${HADOOP_CONF_DIR}/hdfs-site.xml

# Crear directorios de datos
RUN mkdir -p /hadoop/dfs/name /hadoop/dfs/data /opt/hadoop/tmp /opt/hadoop/logs

# Formatear NameNode
RUN ${HADOOP_HOME}/bin/hdfs namenode -format -force -nonInteractive

# Volúmenes de persistencia
VOLUME ["/hadoop/dfs/name", "/hadoop/dfs/data"]

# Puertos expuestos (HDFS only)
# 9870: NameNode Web UI
# 9864: DataNode Web UI
# 9000: HDFS RPC
# 9866: DataNode data transfer
EXPOSE 9870 9864 9000 9866

WORKDIR ${HADOOP_HOME}

# Script de inicio
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]

