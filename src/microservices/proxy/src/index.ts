import express from 'express';
import { loadConfig } from './config';
import { createRoutes } from './routes';

const config = loadConfig();
const app = express();

app.use(createRoutes(config));

app.listen(config.port, () => {
  console.log(`Proxy service listening on port ${config.port}`);
  console.log(`Monolith: ${config.monolithUrl}`);
  console.log(`Movies Service: ${config.moviesServiceUrl}`);
  console.log(`Events Service: ${config.eventsServiceUrl}`);
  console.log(
    `Gradual migration: ${config.gradualMigration}, movies migration: ${config.moviesMigrationPercent}%`,
  );
});
