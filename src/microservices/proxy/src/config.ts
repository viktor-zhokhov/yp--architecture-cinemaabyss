import { ProxyConfig } from './types';

export function loadConfig(): ProxyConfig {
  return {
    port: parseInt(process.env.PORT || '8000', 10),
    monolithUrl: process.env.MONOLITH_URL || 'http://monolith:8080',
    moviesServiceUrl: process.env.MOVIES_SERVICE_URL || 'http://movies-service:8081',
    eventsServiceUrl: process.env.EVENTS_SERVICE_URL || 'http://events-service:8082',
    gradualMigration: process.env.GRADUAL_MIGRATION === 'true',
    moviesMigrationPercent: parseInt(process.env.MOVIES_MIGRATION_PERCENT || '0', 10),
  };
}
