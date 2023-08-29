const { Kafka } = require('kafkajs')

const kafka = new Kafka({
  clientId: 'kafka-demo',
  brokers: ['localhost:9092'],
  ssl: false,
  sasl: {
    mechanism: 'plain',
    username: 'admin',
    password: 'admin-secret'
  },
});

const admin = kafka.admin();

async function createTopicAndPartition() {
  try {
    await admin.connect();

    const res = await admin.createTopics({
      topics: [{topic: 'test-topic'}],
    });

    await admin.createPartitions({
      topicPartitions: [{
        topic: 'test-topic',
        count: 2,
      }],
    });

    const metatdata = await admin.fetchTopicMetadata({ topics: ['test-topic'] });

    await admin.disconnect();
  } catch(error) {
    console.log(error);
  }
}


createTopicAndPartition();