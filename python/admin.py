from confluent_kafka.admin import AdminClient, NewTopic

admin = AdminClient({
  'bootstrap.servers' : 'localhost:9092',
  'client.id'         : 'kafka-demo',
  'security.protocol' : 'SASL_PLAINTEXT',
  'sasl.mechanism'    : 'PLAIN',
  'sasl.username'     : 'admin',
  'sasl.password'     : 'admin-secret'
})

newTopic = admin.create_topics([NewTopic(topic = 'test-topic', num_partitions = 2)])

print(newTopic)
