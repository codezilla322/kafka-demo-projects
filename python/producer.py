from confluent_kafka import Producer
import sys

producer = Producer({
  'bootstrap.servers' : 'localhost:9092',
  'client.id'         : 'kafka-demo',
  'security.protocol' : 'SASL_PLAINTEXT',
  'sasl.mechanism'    : 'PLAIN',
  'sasl.username'     : 'test',
  'sasl.password'     : 'test-secret'
})

clusterMetaData = producer.list_topics()
print(clusterMetaData.topics)

def delivery_callback(err, msg):
  if err:
    sys.stderr.write('%% Message failed delivery: %s\n' % err)
  else:
    sys.stderr.write('%% Message delivered to %s [%d] @ %d\n' % (msg.topic(), msg.partition(), msg.offset()))

producer.poll(0);

producer.produce(topic = "test-topic", value = "hello world!", key = "key1", partition = 0, callback = delivery_callback)
producer.produce(topic = "test-topic", value = "hey hey!", key = "key2", partition = 0, callback = delivery_callback)
producer.produce(topic = "test-topic", value = "hi hi!", key = "key3", partition = 1, callback = delivery_callback)

producer.flush();