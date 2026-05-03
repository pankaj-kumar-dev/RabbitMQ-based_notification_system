export const EXCHANGE = 'notifications.exchange';
export const RETRY_EXCHANGE = 'notifications.retry.exchange';
export const DLQ_EXCHANGE = 'notifications.dlq.exchange';

export const QUEUES = {
  EMAIL_1: 'email.queue.1',
  EMAIL_2: 'email.queue.2',
  SMS_1: 'sms.queue.1',
  RETRY_1S: 'retry_1s',
  RETRY_5S: 'retry_5s',
  RETRY_30S: 'retry_30s',
  DLQ: 'notifications.dlq',
} as const;

export const ROUTING_KEYS = {
  EMAIL_1: 'email.1',
  EMAIL_2: 'email.2',
  SMS_1: 'sms.1',
  RETRY_1S: 'retry.1s',
  RETRY_5S: 'retry.5s',
  RETRY_30S: 'retry.30s',
  DLQ: 'dlq',
} as const;
