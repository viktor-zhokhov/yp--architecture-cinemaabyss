import { EventsConfig } from './types';

export function loadConfig(): EventsConfig {
  return {
    port: parseInt(process.env.PORT || '8082', 10),
    kafkaBrokers: (process.env.KAFKA_BROKERS || 'kafka:9092').split(','),
  };
}
