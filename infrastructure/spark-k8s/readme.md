

******

<details>
<summary>Minikube</summary>
<br />

Minikube es una herramienta que se utiliza para ejecutar localmente un clúster de Kubernetes de un solo nodo.

Siga el redme  de instalación de Minikube para instalarlo junto con un hipervisor (como VirtualBox o HyperKit), para administrar máquinas virtuales, y Kubectl, para implementar y administrar aplicaciones en Kubernetes.

De manera predeterminada, la máquina virtual Minikube está configurada para usar 1 GB de memoria y 2 núcleos de CPU. Esto no es suficiente para los trabajos de Spark, así que asegúrese de aumentar la memoria en su cliente Docker (para HyperKit) o ​​directamente en VirtualBox. Luego, cuando inicie Minikube, pásele las opciones de memoria y CPU:

```sh

$ minikube start --vm-driver=hyperkit --memory 8192 --cpus 4

```

or

```

$ minikube start --memory 8192 --cpus 4

```
</details>

******

******

<details>
<summary>Docker</summary>
<br />

A continuación, construyamos una imagen de Docker personalizada para Spark 3.2.0, diseñada para el modo autónomo de Spark.

>Dockerfile:

```sh
# base image
FROM openjdk:11

# define spark and hadoop versions
ENV SPARK_VERSION=3.2.0
ENV HADOOP_VERSION=3.3.1

# download and install hadoop
RUN mkdir -p /opt && \
    cd /opt && \
    curl http://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz | \
        tar -zx hadoop-${HADOOP_VERSION}/lib/native && \
    ln -s hadoop-${HADOOP_VERSION} hadoop && \
    echo Hadoop ${HADOOP_VERSION} native libraries installed in /opt/hadoop/lib/native

# download and install spark
RUN mkdir -p /opt && \
    cd /opt && \
    curl http://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop2.7.tgz | \
        tar -zx && \
    ln -s spark-${SPARK_VERSION}-bin-hadoop2.7 spark && \
    echo Spark ${SPARK_VERSION} installed in /opt

# add scripts and update spark default config
ADD common.sh spark-master spark-worker /
ADD spark-defaults.conf /opt/spark/conf/spark-defaults.conf
ENV PATH $PATH:/opt/spark/bin

```


Puede encontrar el Dockerfile anterior junto con el archivo de configuración de Spark y las secuencias de comandos en el repositorio de spark-kubernetes en GitHub.


>Build the image:


```sh
$ eval $(minikube docker-env)
$ docker build -f docker/Dockerfile -t spark-hadoop:3.2.0 ./docker

```

</details>

******

******

<details>
<summary> Spark Master</summary>
<br />

>spark-master-deployment.yaml:

```yaml
kind: Deployment
apiVersion: apps/v1
metadata:
  name: spark-master
spec:
  replicas: 1
  selector:
    matchLabels:
      component: spark-master
  template:
    metadata:
      labels:
        component: spark-master
    spec:
      containers:
        - name: spark-master
          image: spark-hadoop:3.2.0
          command: ["/spark-master"]
          ports:
            - containerPort: 7077
            - containerPort: 8080
          resources:
            requests:
              cpu: 100m

```

>spark-master-service.yaml:

```yaml
kind: Service
apiVersion: v1
metadata:
  name: spark-master
spec:
  ports:
    - name: webui
      port: 8080
      targetPort: 8080
    - name: spark
      port: 7077
      targetPort: 7077
  selector:
    component: spark-master
```
Cree la implementación maestra de Spark e inicie los servicios:
```sh
$ kubectl create -f ./kubernetes/spark-master-deployment.yaml
$ kubectl create -f ./kubernetes/spark-master-service.yaml

```
Verificar:

```sh
$ kubectl get deployments

NAME           READY   UP-TO-DATE   AVAILABLE   AGE
spark-master   1/1     1            1           2m55s


$ kubectl get pods

NAME                          READY   STATUS    RESTARTS   AGE
spark-master-dbc47bc9-tlgfs   1/1     Running   0          3m8s

```
</details>

******

******

<details>
<summary>Spark Workers</summary>
<br />

>spark-worker-deployment.yaml:
```yaml
kind: Deployment
apiVersion: apps/v1
metadata:
  name: spark-worker
spec:
  replicas: 2
  selector:
    matchLabels:
      component: spark-worker
  template:
    metadata:
      labels:
        component: spark-worker
    spec:
      containers:
        - name: spark-worker
          image: spark-hadoop:3.2.0
          command: ["/spark-worker"]
          ports:
            - containerPort: 8081
          resources:
            requests:
              cpu: 100m

```

>Cree la implementación del trabajador de Spark:
```sh
$ kubectl create -f ./kubernetes/spark-worker-deployment.yaml
```

> Verificar:
```sh
$ kubectl get deployments

NAME           READY   UP-TO-DATE   AVAILABLE   AGE
spark-master   1/1     1            1           6m35s
spark-worker   2/2     2            2           7s


$ kubectl get pods

NAME                            READY   STATUS    RESTARTS   AGE
spark-master-dbc47bc9-tlgfs     1/1     Running   0          6m53s
spark-worker-795dc47587-fjkjt   1/1     Running   0          25s
spark-worker-795dc47587-g9n64   1/1     Running   0          25s

```
</details>

******

******

<details>
<summary>Ingress</summary>
<br />


¿Notó que expusimos la interfaz de usuario web de Spark en el puerto 8080? Para acceder a él fuera del clúster, configuremos un objeto Ingress.

>minikube-ingress.yaml:
```yaml
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: minikube-ingress
  annotations:
spec:
  rules:
  - host: spark-kubernetes
    http:
      paths:
        - pathType: Prefix
          path: /
          backend:
            service:
              name: spark-master
              port:
                number: 8080

```

>Habilite el complemento de ingreso:

```sh
$ minikube addons enable ingress

```
>Cree el objeto de ingreso:

```sh
$ kubectl apply -f ./kubernetes/minikube-ingress.yaml

```


A continuación, debe actualizar su archivo /etc/hosts para enrutar las solicitudes del host que definimos, spark-kubernetes, a la instancia de Minikube.

Agregue una entrada a /etc/hosts:
```sh
$ echo "$(minikube ip) spark-kubernetes" | sudo tee -a /etc/hosts
```

>Pruébelo en el navegador en http://spark-kubernetes/:
</details>

******

******

<details>
<summary>Test</summary>
<br />

Para probar, ejecute el shell PySpark desde el contenedor maestro:
```sh
$ kubectl get pods -o wide

NAME                            READY   STATUS    RESTARTS   AGE     IP           NODE       NOMINATED NODE   READINESS GATES
spark-master-dbc47bc9-t6v84     1/1     Running   0          7m35s   172.17.0.6   minikube   <none>           <none>
spark-worker-795dc47587-5ch8f   1/1     Running   0          7m24s   172.17.0.9   minikube   <none>           <none>
spark-worker-795dc47587-fvcf6   1/1     Running   0          7m24s   172.17.0.7   minikube   <none>           <none>

$ kubectl exec spark-master-dbc47bc9-t6v84 -it -- \
    pyspark --conf spark.driver.bindAddress=172.17.0.6 --conf spark.driver.host=172.17.0.6

```

>
Luego, ejecute el siguiente código después de que aparezca el mensaje de PySpark:
</details>

******

