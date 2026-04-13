export interface ProxyConfig {
  port: number;
  monolithUrl: string;
  moviesServiceUrl: string;
  eventsServiceUrl: string;
  gradualMigration: boolean;
  moviesMigrationPercent: number;
}
