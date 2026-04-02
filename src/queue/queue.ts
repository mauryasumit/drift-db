import type Database from 'better-sqlite3';
import type { SyncJob } from '../types.js';
import { generateId } from '../utils/id.js';
import { nextRetryAt } from '../utils/retry.js';
import type { RetryConfig } from '../types.js';

const SCHEMA = `
  CREATE TABLE IF NOT EXISTS _driftdb_queue (
    id TEXT PRIMARY KEY,
    type TEXT NOT NULL,
    payload TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    attempts INTEGER NOT NULL DEFAULT 0,
    nextRetryAt INTEGER NOT NULL DEFAULT 0,
    createdAt INTEGER NOT NULL,
    error TEXT
  );
  CREATE INDEX IF NOT EXISTS idx_driftdb_queue_status ON _driftdb_queue (status, nextRetryAt);
`;

export class SyncQueue {
  private readonly db: Database.Database;
  private readonly retryConfig: RetryConfig;

  constructor(db: Database.Database, retryConfig: RetryConfig) {
    this.db = db;
    this.retryConfig = retryConfig;
    db.exec(SCHEMA);
  }

  enqueue(type: SyncJob['type'], payload: object): SyncJob {
    const job: SyncJob = {
      id: generateId(),
      type,
      payload: JSON.stringify(payload),
      status: 'pending',
      attempts: 0,
      nextRetryAt: 0,
      createdAt: Date.now(),
      error: null,
    };

    this.db
      .prepare(
        `INSERT INTO _driftdb_queue (id, type, payload, status, attempts, nextRetryAt, createdAt, error)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?)`
      )
      .run(job.id, job.type, job.payload, job.status, job.attempts, job.nextRetryAt, job.createdAt, job.error);

    return job;
  }

  dequeue(limit = 5): SyncJob[] {
    const now = Date.now();
    const rows = this.db
      .prepare(
        `SELECT * FROM _driftdb_queue
         WHERE status IN ('pending', 'failed')
           AND nextRetryAt <= ?
         ORDER BY createdAt ASC
         LIMIT ?`
      )
      .all(now, limit) as SyncJob[];

    if (rows.length === 0) return [];

    const ids = rows.map((r) => r.id);
    const placeholders = ids.map(() => '?').join(',');
    this.db
      .prepare(`UPDATE _driftdb_queue SET status = 'processing' WHERE id IN (${placeholders})`)
      .run(...ids);

    return rows.map((r) => ({ ...r, status: 'processing' as const }));
  }

  markDone(id: string): void {
    this.db
      .prepare(`UPDATE _driftdb_queue SET status = 'done', error = NULL WHERE id = ?`)
      .run(id);
  }

  markFailed(id: string, error: string): void {
    const job = this.db
      .prepare(`SELECT attempts FROM _driftdb_queue WHERE id = ?`)
      .get(id) as Pick<SyncJob, 'attempts'> | undefined;

    if (!job) return;

    const attempts = job.attempts + 1;
    const willRetry = attempts <= this.retryConfig.maxRetries;
    const status = willRetry ? 'failed' : 'failed';
    const retryAt = willRetry ? nextRetryAt(attempts, this.retryConfig) : 0;

    this.db
      .prepare(
        `UPDATE _driftdb_queue
         SET status = ?, attempts = ?, nextRetryAt = ?, error = ?
         WHERE id = ?`
      )
      .run(status, attempts, retryAt, error.slice(0, 1000), id);
  }

  resetStuck(): void {
    const staleThreshold = Date.now() - 5 * 60 * 1000;
    this.db
      .prepare(
        `UPDATE _driftdb_queue
         SET status = 'pending', nextRetryAt = 0
         WHERE status = 'processing' AND createdAt < ?`
      )
      .run(staleThreshold);
  }

  pendingCount(): number {
    const row = this.db
      .prepare(
        `SELECT COUNT(*) as cnt FROM _driftdb_queue WHERE status IN ('pending', 'processing', 'failed')`
      )
      .get() as { cnt: number };
    return row.cnt;
  }

  purgeCompleted(olderThanMs = 24 * 60 * 60 * 1000): void {
    const cutoff = Date.now() - olderThanMs;
    this.db
      .prepare(`DELETE FROM _driftdb_queue WHERE status = 'done' AND createdAt < ?`)
      .run(cutoff);
  }

  hasPendingOfType(type: SyncJob['type']): boolean {
    const row = this.db
      .prepare(
        `SELECT 1 FROM _driftdb_queue WHERE type = ? AND status IN ('pending', 'processing', 'failed') LIMIT 1`
      )
      .get(type) as { 1: number } | undefined;
    return row !== undefined;
  }
}
