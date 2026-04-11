# Utiliza una imagen base de Hadoop
FROM sequenceiq/hadoop-docker:latest

# Etiqueta para especificar al creador de la imagen y proporcionar una descripción
LABEL maintainer="SequenceIQ <federicopfund@gmail.com>" \
      description="Imagen de Hadoop con configuraciones personalizadas"

# Configura las variables de entorno para Hadoop
ENV HADOOP_HOME /usr/local/hadoop
ENV HADOOP_CONF_DIR /usr/local/hadoop/etc/hadoop
ENV PATH $PATH:$HADOOP_HOME/bin

# Copia la configuración personalizada de Hadoop
COPY conf/core-site.xml $HADOOP_CONF_DIR/core-site.xml
COPY conf/hdfs-site.xml $HADOOP_CONF_DIR/hdfs-site.xml
COPY conf/mapred-site.xml $HADOOP_CONF_DIR/mapred-site.xml
COPY conf/yarn-site.xml $HADOOP_CONF_DIR/yarn-site.xml

# Copia la configuración personalizada de Hadoop a las plantillas
COPY conf/core-site.xml $HADOOP_HOME/share/hadoop/common/templates/core-site.xml
COPY conf/hdfs-site.xml $HADOOP_HOME/share/hadoop/hdfs/templates/hdfs-site.xml
COPY conf/mapred-site.xml $HADOOP_HOME/share/hadoop/mapreduce/templates/mapred-site.xml
COPY conf/yarn-site.xml $HADOOP_HOME/share/hadoop/yarn/templates/yarn-site.xml

# Formatea el sistema de archivos HDFS
# RUN $HADOOP_HOME/bin/hdfs namenode -format

# Agrega volúmenes de persistencia para los datos de Hadoop
VOLUME /usr/local/hadoop-data

# Establece el directorio de trabajo
WORKDIR $HADOOP_HOME

