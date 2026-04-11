
docker-compose exec kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic topic1

sudo docker exec -it kafka-kafka-1 /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic topic1 --from-beginning


docker exec -it kafka-kafka-1  /opt/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092

