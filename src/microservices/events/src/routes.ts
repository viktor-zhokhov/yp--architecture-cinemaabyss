import { Router, Request, Response } from 'express';
import { Producer } from 'kafkajs';
import {
  DomainEvent,
  EventResponse,
  MovieEventBody,
  UserEventBody,
  PaymentEventBody,
  TOPICS,
} from './types';
import { publishEvent } from './kafka';

export function createRoutes(producer: Producer): Router {
  const router = Router();

  router.get('/api/events/health', (_req: Request, res: Response) => {
    res.json({ status: true });
  });

  router.get('/health', (_req: Request, res: Response) => {
    res.json({ status: true });
  });

  router.post('/api/events/movie', async (req: Request, res: Response) => {
    try {
      const body = req.body as MovieEventBody;
      if (!body.movie_id || !body.title || !body.action) {
        res.status(400).json({ error: 'movie_id, title, and action are required' });
        return;
      }

      const event: DomainEvent = {
        id: `movie-${body.movie_id}-${body.action}`,
        type: 'movie',
        timestamp: new Date().toISOString(),
        payload: {
          movie_id: body.movie_id,
          title: body.title,
          action: body.action,
          user_id: body.user_id,
          rating: body.rating,
          genres: body.genres,
          description: body.description,
        },
      };

      const { partition, offset } = await publishEvent(producer, TOPICS.movie, event);
      console.log(`[Producer][movie-events] partition=${partition} offset=${offset}`, JSON.stringify(event));

      const response: EventResponse = { status: 'success', partition, offset, event };
      res.status(201).json(response);
    } catch (err) {
      console.error('Error producing movie event:', err);
      res.status(500).json({ error: err instanceof Error ? err.message : 'Unknown error' });
    }
  });

  router.post('/api/events/user', async (req: Request, res: Response) => {
    try {
      const body = req.body as UserEventBody;
      if (!body.user_id || !body.action || !body.timestamp) {
        res.status(400).json({ error: 'user_id, action, and timestamp are required' });
        return;
      }

      const event: DomainEvent = {
        id: `user-${body.user_id}-${body.action}`,
        type: 'user',
        timestamp: body.timestamp,
        payload: {
          user_id: body.user_id,
          username: body.username,
          email: body.email,
          action: body.action,
          timestamp: body.timestamp,
        },
      };

      const { partition, offset } = await publishEvent(producer, TOPICS.user, event);
      console.log(`[Producer][user-events] partition=${partition} offset=${offset}`, JSON.stringify(event));

      const response: EventResponse = { status: 'success', partition, offset, event };
      res.status(201).json(response);
    } catch (err) {
      console.error('Error producing user event:', err);
      res.status(500).json({ error: err instanceof Error ? err.message : 'Unknown error' });
    }
  });

  router.post('/api/events/payment', async (req: Request, res: Response) => {
    try {
      const body = req.body as PaymentEventBody;
      if (!body.payment_id || !body.user_id || body.amount === undefined || !body.status || !body.timestamp) {
        res.status(400).json({ error: 'payment_id, user_id, amount, status, and timestamp are required' });
        return;
      }

      const event: DomainEvent = {
        id: `payment-${body.payment_id}-${body.status}`,
        type: 'payment',
        timestamp: body.timestamp,
        payload: {
          payment_id: body.payment_id,
          user_id: body.user_id,
          amount: body.amount,
          status: body.status,
          timestamp: body.timestamp,
          method_type: body.method_type,
        },
      };

      const { partition, offset } = await publishEvent(producer, TOPICS.payment, event);
      console.log(`[Producer][payment-events] partition=${partition} offset=${offset}`, JSON.stringify(event));

      const response: EventResponse = { status: 'success', partition, offset, event };
      res.status(201).json(response);
    } catch (err) {
      console.error('Error producing payment event:', err);
      res.status(500).json({ error: err instanceof Error ? err.message : 'Unknown error' });
    }
  });

  return router;
}
