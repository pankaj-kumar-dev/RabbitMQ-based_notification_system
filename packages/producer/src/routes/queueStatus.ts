import { Router } from 'express';

const router = Router();

const RABBITMQ_API = process.env.RABBITMQ_MGMT_URL ?? 'http://rabbitmq:15672';
const AUTH = 'Basic ' + Buffer.from('guest:guest').toString('base64');

const QUEUES = ['retry_1s', 'retry_5s', 'retry_30s', 'notifications.dlq'];

async function getQueueDepth(name: string): Promise<number> {
  const encoded = encodeURIComponent(name);
  const res = await fetch(`${RABBITMQ_API}/api/queues/%2F/${encoded}`, {
    headers: { Authorization: AUTH },
  });
  if (!res.ok) return 0;
  const data = (await res.json()) as { messages?: number };
  return data.messages ?? 0;
}

router.get('/queue-status', async (_req, res) => {
  try {
    const [retry1s, retry5s, retry30s, dlq] = await Promise.all(
      QUEUES.map(getQueueDepth),
    );
    res.json({ retry_1s: retry1s, retry_5s: retry5s, retry_30s: retry30s, dlq });
  } catch {
    res.status(500).json({ error: 'Failed to fetch queue depths' });
  }
});

export default router;
