import type { Channel } from 'amqplib';
import { sleep, type Logger } from '@notification/shared';
import { Prisma } from '@prisma/client';
import { prisma } from './db';
import { publishEvent } from './publisher';

const BATCH_SIZE = 50;

interface RawOutboxRow {
  id: string;
  event_type: string;
  payload: Record<string, unknown>;
  status: string;
  retry_count: number;
  created_at: Date;
  processed_at: Date | null;
}

async function processBatch(channel: Channel, logger: Logger): Promise<number> {
  const processed = await prisma.$transaction(
    async (tx: Prisma.TransactionClient) => {
      const rows = await tx.$queryRaw<RawOutboxRow[]>`
        SELECT id, event_type, payload, status, retry_count, created_at, processed_at
        FROM outbox
        WHERE status = 'PENDING'
        ORDER BY created_at ASC
        FOR UPDATE SKIP LOCKED
        LIMIT ${BATCH_SIZE}
      `;

      if (rows.length === 0) return 0;

      const successIds: string[] = [];

      for (const row of rows) {
        try {
          publishEvent(channel, {
            id: row.id,
            event_type: row.event_type,
            payload: row.payload,
            created_at: row.created_at,
          });
          successIds.push(row.id);
          logger.debug({ eventId: row.id, eventType: row.event_type }, 'Event published');
        } catch (err) {
          logger.error({ err, eventId: row.id }, 'Publish failed — event stays PENDING');
        }
      }

      if (successIds.length > 0) {
        await tx.outbox.updateMany({
          where: { id: { in: successIds } },
          data: { status: 'PROCESSED', processed_at: new Date() },
        });
      }

      return rows.length;
    },
    { timeout: 30_000 },
  );

  return processed;
}

export async function startPoller(channel: Channel, logger: Logger): Promise<void> {
  logger.info('Outbox poller started');

  while (true) {
    try {
      const count = await processBatch(channel, logger);

      if (count > 0) {
        logger.info({ count }, 'Batch processed');
      }

      // Drain loop: keep fetching until outbox is empty
      if (count < BATCH_SIZE) {
        await sleep(1_000);
      }
    } catch (err) {
      logger.error({ err }, 'Poller iteration failed');
      await sleep(5_000);
    }
  }
}
