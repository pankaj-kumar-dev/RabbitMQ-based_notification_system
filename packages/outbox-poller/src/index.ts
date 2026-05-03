import amqp from 'amqplib';
import type { Channel } from 'amqplib';
import { createLogger, config, sleep, setupTopology } from '@notification/shared';
import { prisma } from './db';
import { startPoller } from './poller';

const logger = createLogger('outbox-poller');

// Derive the connection type from the actual return of amqp.connect()
// This avoids the ChannelModel vs Connection naming mismatch in @types/amqplib
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

async function waitForDatabase(): Promise<void> {
  for (let attempt = 1; attempt <= 10; attempt++) {
    try {
      await prisma.$queryRaw`SELECT 1`;
      logger.info('Connected to database');
      return;
    } catch (err) {
      if (attempt === 10) throw err;
      logger.warn({ attempt }, 'Database not ready — retrying');
      await sleep(2_000);
    }
  }
}

async function main(): Promise<void> {
  await waitForDatabase();

  const connection: Connection = await connectRabbitMQ();
  const channel: Channel = await connection.createChannel();

  await setupTopology(channel);
  logger.info('RabbitMQ topology asserted');

  connection.on('error', (err: Error) => {
    logger.error({ err }, 'RabbitMQ connection error');
    process.exit(1);
  });

  connection.on('close', () => {
    logger.warn('RabbitMQ connection closed');
    process.exit(1);
  });

  async function shutdown(): Promise<void> {
    logger.info('Shutting down...');
    try {
      await channel.close();
      await connection.close();
      await prisma.$disconnect();
    } catch (err) {
      logger.error({ err }, 'Error during shutdown');
    }
    process.exit(0);
  }

  process.once('SIGINT', () => void shutdown());
  process.once('SIGTERM', () => void shutdown());

  await startPoller(channel, logger);
}

main().catch((err: unknown) => {
  logger.error({ err }, 'Fatal error');
  process.exit(1);
});
