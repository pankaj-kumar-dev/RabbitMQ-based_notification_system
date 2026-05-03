import { randomUUID } from 'node:crypto';
import { Router } from 'express';
import { z } from 'zod';
import { createLogger } from '@notification/shared';
import { prisma } from '../db';

const router = Router();
const logger = createLogger('producer');

const NotifySchema = z.object({
  type: z.enum(['email', 'sms']),
  payload: z.record(z.unknown()),
});

router.post('/notify', async (req, res) => {
  const parsed = NotifySchema.safeParse(req.body);

  if (!parsed.success) {
    res.status(400).json({ error: parsed.error.flatten() });
    return;
  }

  const { type, payload } = parsed.data;
  const correlationId = randomUUID();
  const eventId = randomUUID();

  try {
    await prisma.outbox.create({
      data: {
        id: eventId,
        event_type: type,
        payload: { ...payload, correlationId },
        status: 'PENDING',
      },
    });

    logger.info({ eventId, correlationId, type }, 'Event written to outbox');

    res.status(202).json({ eventId, correlationId });
  } catch (err) {
    logger.error({ err, correlationId }, 'Failed to write event to outbox');
    res.status(500).json({ error: 'Internal server error' });
  }
});

export default router;
