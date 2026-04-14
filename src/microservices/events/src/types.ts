export interface EventsConfig {
  port: number;
  kafkaBrokers: string[];
}

export interface EventPayload {
  [key: string]: string | number | boolean | string[] | undefined;
}

export interface DomainEvent {
  id: string;
  type: string;
  timestamp: string;
  payload: EventPayload;
}

export interface EventResponse {
  status: string;
  partition: number;
  offset: number;
  event: DomainEvent;
}

export interface MovieEventBody {
  movie_id: number;
  title: string;
  action: string;
  user_id?: number;
  rating?: number;
  genres?: string[];
  description?: string;
}

export interface UserEventBody {
  user_id: number;
  username?: string;
  email?: string;
  action: string;
  timestamp: string;
}

export interface PaymentEventBody {
  payment_id: number;
  user_id: number;
  amount: number;
  status: string;
  timestamp: string;
  method_type?: string;
}

export const TOPICS = {
  movie: 'movie-events',
  user: 'user-events',
  payment: 'payment-events',
} as const;

export type TopicKey = keyof typeof TOPICS;
