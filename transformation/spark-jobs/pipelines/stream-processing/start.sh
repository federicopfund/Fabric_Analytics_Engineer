# Start the ZooKeeper service
bin/zookeeper-server-start.sh config/zookeeper.properties

# Start the Kafka broker service
bin/kafka-server-start.sh config/server.properties
KAFKA_CLUSTER_ID="$(bin/kafka-storage.sh random-uuid)"
bin/kafka-storage.sh format -t $KAFKA_CLUSTER_ID -c config/kraft/server.properties
bin/kafka-server-start.sh config/kraft/server.properties
bin/kafka-topics.sh --create --topic weather --bootstrap-server localhost:9092
bin/kafka-topics.sh --describe --topic weather --bootstrap-server localhost:9092
bin/kafka-console-producer.sh --topic weather --bootstrap-server localhost:9092
bin/kafka-console-consumer.sh --topic weather --from-beginning --bootstrap-server localhost:9092