import Redis from 'ioredis';
import { config } from '@notification/shared';

export function createRedisClient(): Redis {
  const client = new Redis(config.redis.url, {
    lazyConnect: true,
    maxRetriesPerRequest: 3,
  });

  return client;
}

export async function claimEvent(redis: Redis, eventId: string): Promise<boolean> {
  const result = await redis.set(`idempotency:${eventId}`, '1', 'EX', 86400, 'NX');
  return result === 'OK';
}
