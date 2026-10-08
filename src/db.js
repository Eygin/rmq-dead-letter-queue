import { DatabaseSync } from 'node:sqlite';
import { mkdirSync } from 'node:fs';
import { dirname } from 'node:path';
import { config } from './config.js';

mkdirSync(dirname(config.sqlitePath), { recursive: true });

const db = new DatabaseSync(config.sqlitePath);
db.exec('PRAGMA journal_mode = WAL');

db.exec(`
  CREATE TABLE IF NOT EXISTS dlq_messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    dedupe_key TEXT NOT NULL UNIQUE,
    queue TEXT NOT NULL,
    exchange TEXT,
    route TEXT,
    message_id TEXT,
    tenant_id TEXT,
    company_id TEXT,
    store_id TEXT,
    ref_nos TEXT,
    headers TEXT,
    payload BLOB
  )
`);

db.exec('CREATE INDEX IF NOT EXISTS idx_dlq_messages_ref_nos ON dlq_messages(ref_nos)');
db.exec('CREATE INDEX IF NOT EXISTS idx_dlq_messages_tenant_id ON dlq_messages(tenant_id)');
db.exec('CREATE INDEX IF NOT EXISTS idx_dlq_messages_company_id ON dlq_messages(company_id)');
db.exec('CREATE INDEX IF NOT EXISTS idx_dlq_messages_store_id ON dlq_messages(store_id)');

const insertStmt = db.prepare(`
  INSERT OR IGNORE INTO dlq_messages
    (dedupe_key, queue, exchange, route, message_id, tenant_id, company_id, store_id, ref_nos, headers, payload)
  VALUES
    (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
`);

export function insertMessage(row) {
  insertStmt.run(
    row.dedupeKey,
    row.queue,
    row.exchange,
    row.route,
    row.messageId,
    row.tenantId,
    row.companyId,
    row.storeId,
    row.refNos,
    row.headers,
    row.payload,
  );
}

const pruneStmt = db.prepare('DELETE FROM dlq_messages WHERE id <= (SELECT MAX(id) FROM dlq_messages) - ?');

export function pruneOldRows(maxRows) {
  pruneStmt.run(maxRows);
}
