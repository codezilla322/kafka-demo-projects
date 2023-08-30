from confluent_kafka import Consumer, KafkaException
import sys

consumer = Consumer({
  'bootstrap.servers' : 'localhost:9092',
  'client.id'         : 'kafka-demo',
  'security.protocol' : 'SASL_PLAINTEXT',
  'sasl.mechanism'    : 'PLAIN',
  'sasl.username'     : 'test',
  'sasl.password'     : 'test-secret',
  'group.id'          : 'test-group'
})

def print_assignment(consumer, partitions):
  print('Assignment:', partitions)

consumer.subscribe(['test-topic'], on_assign = print_assignment)

try:
  while True:
    msg = consumer.poll(timeout = 1.0)
    if msg is None:
      continue
    if msg.error():
      raise KafkaException(msg.error())
    else:
      sys.stderr.write('%% %s [%d] at offset %d with key %s:\n' % (msg.topic(), msg.partition(), msg.offset(), str(msg.key())))
      print(msg.value())
except KeyboardInterrupt:
  sys.stderr.write('%% Aborted by user\n')
finally:
   consumer.close()