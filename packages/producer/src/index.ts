import path from 'node:path';
import express from 'express';
import { createLogger, config } from '@notification/shared';
import { prisma } from './db';
import notifyRouter from './routes/notify';
import statusRouter from './routes/status';
import queueStatusRouter from './routes/queueStatus';

const logger = createLogger('producer');

const app = express();
app.use(express.json());
app.use(express.static(path.join(__dirname, '../public')));
app.use(notifyRouter);
app.use(statusRouter);
app.use(queueStatusRouter);

app.use(
  (
    err: unknown,
    _req: express.Request,
    res: express.Response,
    _next: express.NextFunction,
  ) => {
    logger.error({ err }, 'Unhandled error');
    res.status(500).json({ error: 'Internal server error' });
  },
);

const server = app.listen(config.producer.port, () => {
  logger.info({ port: config.producer.port }, 'Producer service started');
});

async function shutdown(): Promise<void> {
  logger.info('Shutting down...');
  server.close();
  await prisma.$disconnect();
  process.exit(0);
}

process.once('SIGINT', () => void shutdown());
process.once('SIGTERM', () => void shutdown());
