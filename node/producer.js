const { Kafka, Partitioners } = require('kafkajs')

const kafka = new Kafka({
  clientId: 'kafka-demo',
  brokers: ['localhost:9092'],
  ssl: false,
  sasl: {
    mechanism: 'plain',
    username: 'test',
    password: 'test-secret'
  },
});

const producer = kafka.producer({ createPartitioner: Partitioners.DefaultPartitioner });

async function sendMessage() {
  await producer.connect()
  await producer.send({
    topic: 'test-topic',
    messages: [
      { key: 'key1', value: 'hello world!', partition: 0 },
      { key: 'key2', value: 'hey hey!', partition: 1 },
      { key: 'key3', value: 'hi hi!', partition: 3 }
    ],
  });
}

sendMessage();