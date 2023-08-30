## Kafka with KRaft

```sh
$ KAFKA_CLUSTER_ID="$(bin/kafka-storage.sh random-uuid)"
$ bin/kafka-storage.sh format -t $KAFKA_CLUSTER_ID -c config/kraft/server-sasl-plain.properties
$ export KAFKA_OPTS="-Djava.security.auth.login.config=config/kraft/kafka_server_jaas.conf"
$ bin/kafka-server-start.sh config/kraft/server-sasl-plain.properties
```