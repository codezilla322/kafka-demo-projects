const { Kafka } = require('kafkajs')

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

const consumer = kafka.consumer({ groupId: 'kafka-group-2' });

async function subscribeAndRead() {
  await consumer.connect()

  await consumer.subscribe({ topics: ['test-topic'], fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ topic, partition, message, heartbeat, pause }) => {
      console.log({
        partition: partition,
        key: message.key.toString(),
        value: message.value.toString(),
        headers: message.headers,
      })
  },
  })
}

subscribeAndRead();

