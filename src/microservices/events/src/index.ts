import express from 'express';
import { Producer } from 'kafkajs';
import { loadConfig } from './config';
import { createKafka, startAllConsumers } from './kafka';
import { createRoutes } from './routes';

async function start(): Promise<void> {
  const config = loadConfig();
  const kafka = createKafka(config);

  const producer: Producer = kafka.producer();
  await producer.connect();
  console.log('Kafka producer connected');

  await startAllConsumers(kafka);

  const app = express();
  app.use(express.json());
  app.use(createRoutes(producer));

  app.listen(config.port, () => {
    console.log(`Events service listening on port ${config.port}`);
  });
}

start().catch((err: unknown) => {
  console.error('Failed to start events service:', err);
  process.exit(1);
});
