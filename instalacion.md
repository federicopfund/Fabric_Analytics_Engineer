
******

<details>
<summary>Instalacion</summary>
<br />

## Instalacion de `Java` y `Scala`:

> Si no tienes instalado el jdk de java:
```bash
sudo apt update

sudo apt install openjdk-11-jdk 

java --version
```
 `Output:`
```bash
openjdk 11.0.17 2022-10-18
OpenJDK Runtime Environment (build 11.0.17+8-post-Ubuntu-1ubuntu222.04)
OpenJDK 64-Bit Server VM (build 11.0.17+8-post-Ubuntu-1ubuntu222.04, mixed mode, sharing)
```
> Instalar Scala:  
```bash
sudo apt install scala
scala -version
```
`Output:`
```bash
Scala code runner version 2.11.12 -- Copyright 2002-2017, LAMP/EPFL
```

## Instalar `Apache Spark`
 
>Descargar la última versión.

[link](https://archive.apache.org/dist/spark/spark-3.3.1/)

```bash
cd /tmp
wget https://archive.apache.org/dist/spark/spark-3.3.1/spark-3.3.1-bin-hadoop3.tgz 
```

> Extraiga el archivo descargado y muévalo al `/opt` directorio, renombrado como `spark` e elimina de archivos temporales el relese.
```bash
tar -xvzf spark-3.3.1-bin-hadoop3.tgz 
sudo mv spark-3.3.1-bin-hadoop3 /opt/spark
cd /opt/spark 
```
>Cree variables de entorno para poder ejecutar y ejecutar `Spark`:
```bash
nano ~/.bashrc
```
>Agregue las líneas en la parte inferior del archivo y guárdelo.
```bash
export SPARK_HOME=/opt/spark

export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
```
>Ejecute los siguientes comandos para aplicar los cambios de su entorno.
```bash
source ~/.bashrc
```
</details>

******


******

<details>
<summary>Inicie Apache Spark.</summary>
<br />

## Inicie Apache `Spark`.
>En este punto, Apache Spark está instalado y listo para usar. 
> Ejecute los siguientes comandos para iniciarlo.
```bash
cd /opt/spark

./sbin/start-master.sh
```
>Inicie el proceso de trabajo de Spark ejecutando los siguientes comandos.
```
./sbin/start-slave.sh spark://localhost:7077
```
>Puede reemplazar el host localhost con el nombre de host del servidor o la dirección IP.
```
http://localhost:8080
```

>Si deseas conectarse a Spark a través de su shell de comandos, ejecute los siguientes comandos:
```
./sbin/spark-shell
```

</details>

******

<details>
<summary>Path de data.</summary>
<br />

## Path de data:
> Ahora Moveremos la careta data al lugar indicado: seguramente te encuentras en el `path:`  /opt/spark:
> Pocisionate en la carpeta donde esta data Osea:

```
cd --
cd /home/fede/data-engineer/transformation/spark-jobs/pipelines/batch-etl-scala/src/main/resources/csv
```
>Es posible que veas la carpeta data:
>Ahora mueve esa carpeta a: `opt/spark/spark-warehouse.`

```
mv data ../../../../opt/spark/spark-warehouse
```
>Ahora  vuelve a: `opt/spark`:
```
cd /opt/spark
```
>Analiza el directorio, veras las carpeta `csv` dentro de `spark-warehouse.` con todos los archivos a tranformar. asegurate que se encuentre dicha carpeta. recuerda la carpeta csv dentro de   `spark-warehouse.`.

```
ls -l
```

</details>

******
<details>
<summary>Run ETL Local</summary>
<br />

## Run `ETL` Local
>Run Spark App si el clon fuen en Documents.

```
./bin/spark-submit \
                --master spark://localhost:7077 \
                 $HOME/Documents/notebooks/transformation/spark-jobs/pipelines/batch-etl-python/etl.py

```
>En mi caso solo para el archivo VentasInternet.csv:
```
./bin/spark-submit \
                --master spark://fede:7077 \
                 $HOME/Documents/notebooks/transformation/spark-jobs/pipelines/batch-etl-python/etl.py VentasInternet.csv

```
> Si quiere ejecutar todas las tranformaciones con el siguiente comando:

```
./bin/spark-submit --master spark://fede:7077 \
                    $HOME/Documents/notebooks/transformation/spark-jobs/pipelines/batch-etl-python/etl.py\
                     VentasInternet.csv \
                     Categoria.csv \
                     FactMine.csv \
                     Producto.csv \
                     Mine.csv

```
> Standalone app modo cliente y  cluster:

```
spark-submit  
    --class main.MainETL \
    --master spark://10.0.2.15:7077 \
    --deploy-mode client \
    --jars /opt/spark/jars/delta-core_2.12-2.1.0.jar,/opt/spark/jars/delta-storage-2.1.0.jar \
    --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension"  \
    --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
     target/scala-2.12/root_2.12-0.0.1.jar
```
> Standalone app modo  cluster:

```
spark-submit  
 --class main.MainETL  
 --master spark://10.0.2.15:7077  
 --deploy-mode cluster  
 --executor-cores 1  
 --executor-memory 2G  
 --driver-memory 1G  
 target/scala-2.12/root_2.12-0.0.1.jar



```
> Recuerda tienes que estar parado en el directorio ```/opt/spark ```y tener la carpeta de ```csv``` proveniente de ```data``` en 
```spark-warehouse```.

> Tiempo de ejecucion total: ```36.316101``` ms.  tranquilo bucaremos mayor eficiencia.

</details>


</details>

******

<details>
<summary>Hadoop install</summary>
<br />

### Para leer las tablas de un Nodo Storage en Hadoop
>### Hadoop install

>Comensamos con un:
```
sudo apt update

```
>Verificamos si java esta en el pack
```
echo #JAVA_HOME

```
>No se encuentra entonces los vamos a agregar al pack
```
cd /usr/lib/jvm

```
>Listamos con el comando
```
ls

```
>Vamos a agregar java al pack usando su ubicacion
```
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
```
>Verificamos si ya esta en el pack

```
echo $JAVA_HOME

```
>Instalamos ssh para el acceso remoto a los servidores
```
sudo apt-get install ssh

```
>Instalamos pdsh para ejecutar multiples comandos remotos
```
sudo apt-get install pdsh

```
>Vamos a la pagina oficial de hadoop para su descarga


![link](http://hadoop.apache.org/)

>Download: Vamos a utiliar la version 3.2.1 damos clic en Binary para descargar.
>Nos dirigimos a la carpeta descargas.

>cd 
```
cd Downloads

```
>ls para ver el binario y descomprimimos el archivo de hadoop.
```
tar -zxvf hadoop-3.3.4.tar.gz

```
> Renombramos la carpeta y la movemos al diractorio opt:
```sh
mv hadoop-3.3.4 a /opt/hadoop

```

> luego verificamos que se creo  haya creado Hadoop.
```sh
ls

```

> Editamos el archivo hadoop-env.sh.
```sh
nano hadoop-env.sh

```
> Damos control f para buscar la palabra export e insertamos el path de JAVA_HOME 

```sh
export JAVA_HOME="/usr/lib/jvm/java-11-openjdk-amd64"

```

> Volvemos a la carpeta principal de hadoop.

```sh
cd ../..

```
> Ejecutamos el comando:
```sh
bin/hadoop
```
> Se muestra la documentacion del Script hadoop

```sh
bin/hadoop
```

> Volvemos a la documentacion a Single Nodo Setup e insertamos la siguiente configuracion en el archivo `core-site.xml`.

```xml
<configuration>
    <property>
        <name>fs.defaultFS</name>
        <value>hdfs://localhost:9000</value>
    </property>
</configuration>
```

> Volvemos a la documentacion a Single Nodo Setup e insertamos la siguiente configuracion en el archivo `hdfs-site.xml`.

```xml
<configuration>
    <property>
        <name>dfs.replication</name>
        <value>1</value>
    </property>
</configuration>
```

> Guardamos cambios.

> Otorgamos acceso ssh al localhost sin frase y contraseña.

```sh
ssh localhost
ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa
```
> Se nos genera una clavee:
```sh
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
```
> Para los permisos.

```sh
chmod 0600 ~/.ssh/authorized_keys
```
> Configurar los archivos para correr un modelo de programacion.
```sh
bin/hdfs namenode -format
```
```sh
export PDSH_RCMD_TYPE=ssh
```

> Vamos a iniciar los diamon.
```sh
sbin/start-dfs.sh
```

>Vamos a Nuestro navegador para ver la interfas 
```sh
localhost:9870
```
>detenemos deamon
```sh
sbin/stop-dfs.sh
```
> Editamos el archivo:
```sh
nano mapred-site.xml
```
> Vamos a la paguina de la documentacion y copiamos el codigo en mapred-site.xml
```xml

<configuration>
    <property>
        <name>mapreduce.framework.name</name>
        <value>yarn</value>
    </property>
    <property>
        <name>mapreduce.application.classpath</name>
        <value>$HADOOP_MAPRED_HOME/share/hadoop/mapreduce/*:$HADOOP_MAPRED_HOME/share/hadoop/mapreduce/lib/*</value>
    </property>
</configuration>
```


</details>

******

### Hadoop Commandos

<br />

**Comandos utilizados Hadoop**
<details>
<summary>Comandos </summary>
<br />

`hdfs dfs -chmod -R 777 /hive/warehouse/datalake/bronze` = Borra los elementos del directorio

`hdfs dfs -mkdir /user/fede/input/csv` = Crea un directorio `csv`. 

`hdfs dfs -ls /` = Muestra el directorio. 

`hdfs dfs -put csv /user/fede/input` = Empuja la carpeta de archivo csv a la carpeta Input.

>output:
`-rw-r--r--   1 fede supergroup    1598678 2023-01-12 19:01 /user/fede/input/csv` 

`hdfs dfs -ls /user/fede/input` = Muestra las carpetas en el directorio. 

`hdfs dfs -cp /user/fede/input/csv/Mine.csv /user/fede/Output/landing` = Mueve el archivo Mine.csv al ente directorio landing. 

`hdfs dfs -rm /user/fede/input/csv/Mine.csv` = Remueve el archivo Mine.csv de la carpeta csv.


<br />
