import { Router } from 'express';
import { prisma } from '../db';

const router = Router();

router.get('/status', async (_req, res) => {
  try {
    const events = await prisma.outbox.findMany({
      orderBy: { created_at: 'desc' },
      take: 50,
      select: {
        id: true,
        event_type: true,
        status: true,
        created_at: true,
        processed_at: true,
      },
    });
    res.json({ events });
  } catch (err) {
    res.status(500).json({ error: 'Failed to fetch outbox status' });
  }
});

export default router;
