import { Router, Request, Response, NextFunction } from 'express';
import { createProxyMiddleware, Options } from 'http-proxy-middleware';
import { ProxyConfig } from './types';

export function createRoutes(config: ProxyConfig): Router {
  const router = Router();

  // Health check
  router.get('/health', (_req: Request, res: Response) => {
    res.json({ status: true, message: 'Strangler Fig Proxy is healthy' });
  });

  // Strangler Fig: /api/movies → movies-service или monolith
  router.use('/api/movies', (req: Request, res: Response, next: NextFunction) => {
    const useNewService =
      config.gradualMigration && Math.random() * 100 < config.moviesMigrationPercent;

    const target = useNewService ? config.moviesServiceUrl : config.monolithUrl;
    const label = useNewService ? 'movies-service' : 'monolith';
    console.log(`[Strangler Fig] /api/movies → ${label} (${config.moviesMigrationPercent}%)`);

    const proxyOptions: Options = { target, changeOrigin: true, logLevel: 'warn' };
    createProxyMiddleware(proxyOptions)(req, res, next);
  });

  // Events → events-service
  router.use(
    '/api/events',
    createProxyMiddleware({
      target: config.eventsServiceUrl,
      changeOrigin: true,
      logLevel: 'warn',
    }),
  );

  // Всё остальное → monolith (users, payments, subscriptions)
  router.use(
    '/api',
    createProxyMiddleware({
      target: config.monolithUrl,
      changeOrigin: true,
      logLevel: 'warn',
    }),
  );

  return router;
}
