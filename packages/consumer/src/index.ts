import amqp from 'amqplib';
import type { Channel, ConsumeMessage } from 'amqplib';
import {
  createLogger,
  config,
  sleep,
  setupTopology,
  QUEUES,
  type NotificationMessage,
} from '@notification/shared';
import { createRedisClient, claimEvent } from './redis';

const logger = createLogger('consumer');
const FAIL_MODE = process.env.FAIL_MODE === 'true';

type Connection = Awaited<ReturnType<typeof amqp.connect>>;

async function connectRabbitMQ(): Promise<Connection> {
  const MAX_RETRIES = 10;
  for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
    try {
      const conn: Connection = await amqp.connect(config.rabbitmq.url);
      logger.info('Connected to RabbitMQ');
      return conn;
    } catch (err) {
      if (attempt === MAX_RETRIES) throw err;
      const delay = Math.min(1_000 * attempt, 10_000);
      logger.warn({ attempt, delay }, 'RabbitMQ not ready — retrying');
      await sleep(delay);
    }
  }
  throw new Error('Unreachable');
}

async function simulateProcessing(message: NotificationMessage): Promise<void> {
  if (FAIL_MODE) throw new Error('Simulated failure — activating retry chain');
  const delay = 50 + Math.random() * 100;
  await sleep(delay);
  logger.info(
    {
      eventId: message.eventId,
      type: message.type,
      correlationId: message.correlationId,
    },
    'Notification processed',
  );
}

async function main(): Promise<void> {
  const redis = createRedisClient();
  await redis.connect();
  logger.info('Connected to Redis');

  const connection: Connection = await connectRabbitMQ();
  const channel: Channel = await connection.createChannel();

  await setupTopology(channel);
  logger.info('RabbitMQ topology asserted');

  await channel.prefetch(10);

  connection.on('error', (err: Error) => {
    logger.error({ err }, 'RabbitMQ connection error');
    process.exit(1);
  });

  connection.on('close', () => {
    logger.warn('RabbitMQ connection closed');
    process.exit(1);
  });

  // 🔥 Shared handler (no duplication)
  const handler = async (msg: ConsumeMessage | null) => {
    if (msg === null) {
      logger.warn('Consumer cancelled by broker');
      return;
    }

    let eventId = '<unknown>';

    try {
      const raw: unknown = JSON.parse(msg.content.toString());

      if (
        typeof raw !== 'object' ||
        raw === null ||
        typeof (raw as Record<string, unknown>)['eventId'] !== 'string'
      ) {
        logger.error({ content: msg.content.toString() }, 'Malformed message — discarding');
        channel.ack(msg);
        return;
      }

      const message = raw as NotificationMessage;
      eventId = message.eventId;

      const claimed = await claimEvent(redis, eventId);

      if (!claimed) {
        logger.info(
          { eventId, correlationId: message.correlationId },
          'Duplicate — skipping',
        );
        channel.ack(msg);
        return;
      }

      await simulateProcessing(message);
      channel.ack(msg);
    } catch (err) {
      logger.error({ err, eventId }, 'Processing failed — routing to retry');
      channel.nack(msg, false, false);
    }
  };

  // ✅ Consume BOTH partitions
  await channel.consume(QUEUES.EMAIL_1, handler);
  await channel.consume(QUEUES.EMAIL_2, handler);

  logger.info(
    { queues: [QUEUES.EMAIL_1, QUEUES.EMAIL_2] },
    'Consumer started',
  );

  async function shutdown(): Promise<void> {
    logger.info('Shutting down...');
    try {
      await channel.close();
      await connection.close();
      await redis.quit();
    } catch (err) {
      logger.error({ err }, 'Error during shutdown');
    }
    process.exit(0);
  }

  process.once('SIGINT', () => void shutdown());
  process.once('SIGTERM', () => void shutdown());
}

main().catch((err: unknown) => {
  logger.error({ err }, 'Fatal error');
  process.exit(1);
});