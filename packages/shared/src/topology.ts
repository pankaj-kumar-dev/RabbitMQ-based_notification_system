import type { Channel } from 'amqplib';
import { EXCHANGE, RETRY_EXCHANGE, DLQ_EXCHANGE, QUEUES, ROUTING_KEYS } from './constants';

export async function setupTopology(channel: Channel): Promise<void> {
  await channel.assertExchange(EXCHANGE, 'topic', { durable: true });
  await channel.assertExchange(RETRY_EXCHANGE, 'direct', { durable: true });
  await channel.assertExchange(DLQ_EXCHANGE, 'direct', { durable: true });

  // DLQ — no further dead-lettering
  await channel.assertQueue(QUEUES.DLQ, { durable: true });
  await channel.bindQueue(QUEUES.DLQ, DLQ_EXCHANGE, ROUTING_KEYS.DLQ);

  // retry_30s → DLQ after 30 s TTL
  await channel.assertQueue(QUEUES.RETRY_30S, {
    durable: true,
    arguments: {
      'x-message-ttl': 30_000,
      'x-dead-letter-exchange': DLQ_EXCHANGE,
      'x-dead-letter-routing-key': ROUTING_KEYS.DLQ,
    },
  });
  await channel.bindQueue(QUEUES.RETRY_30S, RETRY_EXCHANGE, ROUTING_KEYS.RETRY_30S);

  // retry_5s → retry_30s after 5 s TTL
  await channel.assertQueue(QUEUES.RETRY_5S, {
    durable: true,
    arguments: {
      'x-message-ttl': 5_000,
      'x-dead-letter-exchange': RETRY_EXCHANGE,
      'x-dead-letter-routing-key': ROUTING_KEYS.RETRY_30S,
    },
  });
  await channel.bindQueue(QUEUES.RETRY_5S, RETRY_EXCHANGE, ROUTING_KEYS.RETRY_5S);

  // retry_1s → retry_5s after 1 s TTL
  await channel.assertQueue(QUEUES.RETRY_1S, {
    durable: true,
    arguments: {
      'x-message-ttl': 1_000,
      'x-dead-letter-exchange': RETRY_EXCHANGE,
      'x-dead-letter-routing-key': ROUTING_KEYS.RETRY_5S,
    },
  });
  await channel.bindQueue(QUEUES.RETRY_1S, RETRY_EXCHANGE, ROUTING_KEYS.RETRY_1S);

  // Main queues — dead-letter to retry_1s
  const mainArgs = {
    durable: true,
    arguments: {
      'x-dead-letter-exchange': RETRY_EXCHANGE,
      'x-dead-letter-routing-key': ROUTING_KEYS.RETRY_1S,
    },
  };

  await channel.assertQueue(QUEUES.EMAIL_1, mainArgs);
  await channel.bindQueue(QUEUES.EMAIL_1, EXCHANGE, ROUTING_KEYS.EMAIL_1);

  await channel.assertQueue(QUEUES.EMAIL_2, mainArgs);
  await channel.bindQueue(QUEUES.EMAIL_2, EXCHANGE, ROUTING_KEYS.EMAIL_2);

  await channel.assertQueue(QUEUES.SMS_1, mainArgs);
  await channel.bindQueue(QUEUES.SMS_1, EXCHANGE, ROUTING_KEYS.SMS_1);
}
