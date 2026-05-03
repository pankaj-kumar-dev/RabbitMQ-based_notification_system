import pino from 'pino';

export type Logger = pino.Logger;

export function createLogger(service: string): Logger {
  return pino({
    name: service,
    level: process.env['LOG_LEVEL'] ?? 'info',
    timestamp: pino.stdTimeFunctions.isoTime,
    formatters: {
      level: (label: string) => ({ level: label }),
    },
  });
}
