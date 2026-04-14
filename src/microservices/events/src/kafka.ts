import { Kafka, Producer, Consumer, RecordMetadata, EachMessagePayload } from 'kafkajs';
import { EventsConfig, DomainEvent, TopicKey, TOPICS } from './types';

export function createKafka(config: EventsConfig): Kafka {
  return new Kafka({
    clientId: 'events-service',
    brokers: config.kafkaBrokers,
    retry: { initialRetryTime: 3000, retries: 10 },
  });
}

export async function publishEvent(
  producer: Producer,
  topic: string,
  event: DomainEvent,
): Promise<{ partition: number; offset: number }> {
  const result: RecordMetadata[] = await producer.send({
    topic,
    messages: [{ key: event.id, value: JSON.stringify(event) }],
  });
  const { partition, baseOffset } = result[0];
  return { partition, offset: parseInt(baseOffset || '0', 10) };
}

async function startConsumer(kafka: Kafka, type: TopicKey): Promise<void> {
  const topic = TOPICS[type];
  const consumer: Consumer = kafka.consumer({ groupId: `events-${type}-group` });

  await consumer.connect();
  await consumer.subscribe({ topic, fromBeginning: false });
  await consumer.run({
    eachMessage: async ({ topic, partition, message }: EachMessagePayload) => {
      const value = message.value?.toString();
      if (!value) return;

      const event: DomainEvent = JSON.parse(value);
      console.log(
        `[Consumer][${topic}] partition=${partition} offset=${message.offset} event=`,
        JSON.stringify(event),
      );
    },
  });

  console.log(`Consumer started for topic: ${topic}`);
}

export async function startAllConsumers(kafka: Kafka): Promise<void> {
  const topicKeys = Object.keys(TOPICS) as TopicKey[];
  for (const key of topicKeys) {
    await startConsumer(kafka, key);
  }
}
