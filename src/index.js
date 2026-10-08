import amqplib from 'amqplib';
import { createHash } from 'node:crypto';
import { setTimeout as delay } from 'node:timers/promises';
import { config } from './config.js';
import { insertMessage, pruneOldRows } from './db.js';

function dedupeKeyFor(msg) {
  if (msg.properties.messageId) return msg.properties.messageId;
  return createHash('sha256')
    .update(msg.fields.exchange)
    .update(msg.fields.routingKey)
    .update(msg.content)
    .digest('hex');
}

function errorMessageOf(err) {
  return err?.message || err?.fields?.replyText || String(err);
}

function parseJsonBody(msg) {
  try {
    return JSON.parse(msg.content.toString('utf8'));
  } catch {
    return null;
  }
}

function archive(msg, queueName) {
  const body = parseJsonBody(msg);
  const data = body?.data || {};

  insertMessage({
    dedupeKey: dedupeKeyFor(msg),
    queue: queueName,
    exchange: msg.fields.exchange || null,
    route: msg.fields.routingKey || null,
    messageId: msg.properties.messageId || null,
    tenantId: data.tenantId ?? data.tenant_id ?? null,
    companyId: data.companyId ?? data.company_id ?? null,
    storeId: data.storeId ?? data.store_id ?? null,
    refNos: data.refNos ? JSON.stringify(data.refNos) : null,
    headers: msg.properties.headers ? JSON.stringify(msg.properties.headers) : null,
    payload: msg.content,
  });
}

const SAFETY_CAP = 10000;

async function openChannelFor(connection, queueName, channels) {
  const channel = await connection.createChannel();
  channel.on('error', (err) => {
    console.error(`[dlq-watcher] channel error on "${queueName}":`, errorMessageOf(err));
    channels.delete(queueName);
  });
  channels.set(queueName, channel);
  return channel;
}

async function getChannelFor(connection, queueName, channels) {
  return channels.get(queueName) || openChannelFor(connection, queueName, channels);
}

async function pollQueue(connection, queueName, channels) {
  const channel = await getChannelFor(connection, queueName, channels);
  const messages = [];
  let getError = null;

  for (let i = 0; i < SAFETY_CAP; i++) {
    let msg;
    try {
      msg = await channel.get(queueName, { noAck: false });
    } catch (err) {
      getError = err;
      break;
    }
    if (msg === false) break;
    messages.push(msg);
  }

  if (messages.length === SAFETY_CAP) {
    console.warn(`[dlq-watcher] hit safety cap of ${SAFETY_CAP} messages in one poll of "${queueName}", some may be missed this round`);
  }

  for (const msg of messages) {
    try {
      archive(msg, queueName);
    } catch (err) {
      console.error(`[dlq-watcher] failed to archive message from "${queueName}", will retry next poll:`, err.message);
    } finally {
      try {
        channel.nack(msg, false, true);
      } catch {
        // channel already closed; broker will requeue unacked messages on its own.
      }
    }
  }

  if (getError) {
    channels.delete(queueName);
    console.error(`[dlq-watcher] "${queueName}" unavailable, will retry next poll: ${errorMessageOf(getError)}`);
  }

  return messages.length;
}

async function poll(connection, channels) {
  let total = 0;
  for (const queueName of config.dlqNames) {
    total += await pollQueue(connection, queueName, channels);
  }
  if (total > 0) pruneOldRows(config.maxRows);
}

async function run(state) {
  const connection = await amqplib.connect(config.rabbitmqUrl);
  state.connection = connection;
  const channels = new Map();

  connection.on('error', (err) => console.error('[dlq-watcher] connection error:', err.message));

  const closed = new Promise((resolve) => connection.on('close', resolve));

  console.log(`[dlq-watcher] watching ${config.dlqNames.map((n) => `"${n}"`).join(', ')}, polling every ${config.pollIntervalMs}ms`);
  const runPoll = () => poll(connection, channels).catch((err) => console.error('[dlq-watcher] poll failed:', err.message));
  runPoll();
  const interval = setInterval(runPoll, config.pollIntervalMs);

  await closed;
  clearInterval(interval);
  state.connection = null;
}

async function main() {
  const controller = new AbortController();
  const { signal } = controller;
  process.on('SIGINT', () => controller.abort());
  process.on('SIGTERM', () => controller.abort());

  const state = { connection: null };
  signal.addEventListener('abort', () => state.connection?.close().catch(() => {}), { once: true });

  while (!signal.aborted) {
    try {
      await run(state);
      if (!signal.aborted) console.warn('[dlq-watcher] connection closed, reconnecting...');
    } catch (err) {
      console.error('[dlq-watcher] connection failed:', err.message);
    }
    if (signal.aborted) break;
    await delay(config.reconnectDelayMs, undefined, { signal }).catch(() => {});
  }

  console.log('[dlq-watcher] shutting down');
}

main();
