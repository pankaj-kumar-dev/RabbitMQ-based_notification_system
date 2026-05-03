export interface NotificationMessage {
  eventId: string;
  type: 'email' | 'sms';
  payload: Record<string, unknown>;
  correlationId: string;
  createdAt: string;
}

export type NotificationType = NotificationMessage['type'];

export type OutboxStatus = 'PENDING' | 'PROCESSED' | 'FAILED';
