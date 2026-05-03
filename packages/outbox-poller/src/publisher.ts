import type { Channel } from 'amqplib';
import { EXCHANGE, ROUTING_KEYS, type NotificationMessage } from '@notification/shared';

interface OutboxRow {
  id: string;
  event_type: string;
  payload: Record<string, unknown>;
  created_at: Date;
}

function partitionHash(str: string): number {
  let hash = 0;
  for (let i = 0; i < str.length; i++) {
    hash = (hash * 31 + str.charCodeAt(i)) >>> 0;
  }
  return hash;
}

function getRoutingKey(eventType: string, eventId: string): string {
  if (eventType === 'sms') return ROUTING_KEYS.SMS_1;
  return ROUTING_KEYS.EMAIL_1;
}

function extractCorrelationId(payload: Record<string, unknown>, fallback: string): string {
  const id = payload['correlationId'];
  return typeof id === 'string' ? id : fallback;
}

export function publishEvent(channel: Channel, row: OutboxRow): void {
  const correlationId = extractCorrelationId(row.payload, row.id);

  const message: NotificationMessage = {
    eventId: row.id,
    type: row.event_type as NotificationMessage['type'],
    payload: row.payload,
    correlationId,
    createdAt: row.created_at.toISOString(),
  };

  const routingKey = getRoutingKey(row.event_type, row.id);
  const content = Buffer.from(JSON.stringify(message));

  const ok = channel.publish(EXCHANGE, routingKey, content, {
    persistent: true,
    contentType: 'application/json',
    correlationId,
    messageId: row.id,
  });

  if (!ok) {
    throw new Error(`Channel write buffer full for event ${row.id}`);
  }
}
