export const config = {
  rabbitmq: {
    url: process.env['RABBITMQ_URL'] ?? 'amqp://guest:guest@localhost:5672',
  },
  redis: {
    url: process.env['REDIS_URL'] ?? 'redis://localhost:6379',
  },
  database: {
    url: process.env['DATABASE_URL'] ?? 'postgresql://postgres:postgres@localhost:5432/notifications',
  },
  producer: {
    port: Number(process.env['PORT'] ?? 3000),
  },
};
