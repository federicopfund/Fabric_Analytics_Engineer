docker build -t sequenceiq/hadoop-docker:fede .
sudo docker network create hadoop-net
sudo docker-compose up
docker exec -it namenode hadoop fs -mkdir /user
docker exec -it namenode hadoop fs -mkdir /user/fede
docker exec -it namenode hadoop fs -mkdir /user/fede/spark-events
docker exec -it namenode hadoop fs -chmod 777 /user/fede/spark-events
docker exec -it namenode hadoop fs -chown fede:hadoop /user/fede/spark-events
docker exec -it namenode hadoop fs -mkdir /user/fede/landing
docker exec -it namenode hadoop fs -mkdir /user/fede/landing/csv
docker exec -it namenode hadoop fs -chmod 777 /user/fede/landing/csv
docker exec -it namenode hadoop fs -chmod 777 /user/fede/landing/csv/transform
docker exec -it namenode hadoop fs -chown fede:hadoop /user/fede/landing/csv/transform
docker exec -it namenode hadoop fs -chown fede:hadoop /user/fede/landing/csv
docker exec -it namenode hdfs dfsadmin -report
docker exec -it namenode hdfs dfsadmin -refreshNodes
