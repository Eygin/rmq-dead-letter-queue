import 'dotenv/config';

const dlqNames = (process.env.DLQ_NAMES || process.env.DLQ_NAME || 'tiktok-order-queue-dead-letters')
  .split(',')
  .map((name) => name.trim())
  .filter(Boolean);

export const config = {
  rabbitmqUrl: process.env.RABBITMQ_URL || 'amqp://guest:guest@localhost:5672',
  dlqNames,
  pollIntervalMs: Number(process.env.POLL_INTERVAL_MS) || 5000,
  reconnectDelayMs: Number(process.env.RECONNECT_DELAY_MS) || 5000,
  sqlitePath: process.env.SQLITE_PATH || './data/dlq-messages.sqlite',
  maxRows: Number(process.env.MAX_ROWS) || 100000,
};
