import type Database from 'better-sqlite3';
import type { ChangeLogEntry, LogBatch } from '../types.js';

const SCHEMA = `
  CREATE TABLE IF NOT EXISTS _driftdb_log (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp INTEGER NOT NULL,
    nodeId TEXT NOT NULL,
    \`table\` TEXT NOT NULL,
    operation TEXT NOT NULL,
    data TEXT,
    synced INTEGER NOT NULL DEFAULT 0
  );
  CREATE INDEX IF NOT EXISTS idx_driftdb_log_synced ON _driftdb_log (synced, sequence);
`;

export class ChangeLog {
  private readonly db: Database.Database;
  private readonly nodeId: string;

  constructor(db: Database.Database, nodeId: string) {
    this.db = db;
    this.nodeId = nodeId;
    db.exec(SCHEMA);
  }

  append(
    table: string,
    operation: ChangeLogEntry['operation'],
    data: Record<string, unknown> | null
  ): number {
    const result = this.db
      .prepare(
        `INSERT INTO _driftdb_log (timestamp, nodeId, \`table\`, operation, data, synced)
         VALUES (?, ?, ?, ?, ?, 0)`
      )
      .run(Date.now(), this.nodeId, table, operation, data ? JSON.stringify(data) : null);

    return Number(result.lastInsertRowid);
  }

  pendingEntries(limit: number): ChangeLogEntry[] {
    return this.db
      .prepare(
        `SELECT * FROM _driftdb_log WHERE synced = 0 ORDER BY sequence ASC LIMIT ?`
      )
      .all(limit) as ChangeLogEntry[];
  }

  markSynced(fromSequence: number, toSequence: number): void {
    this.db
      .prepare(
        `UPDATE _driftdb_log SET synced = 1 WHERE sequence >= ? AND sequence <= ?`
      )
      .run(fromSequence, toSequence);
  }

  pendingCount(): number {
    const row = this.db
      .prepare(`SELECT COUNT(*) as cnt FROM _driftdb_log WHERE synced = 0`)
      .get() as { cnt: number };
    return row.cnt;
  }

  latestSyncedSequence(): number {
    const row = this.db
      .prepare(`SELECT MAX(sequence) as seq FROM _driftdb_log WHERE synced = 1`)
      .get() as { seq: number | null };
    return row.seq ?? 0;
  }

  buildBatch(entries: ChangeLogEntry[]): LogBatch {
    return {
      version: 1,
      nodeId: this.nodeId,
      fromSequence: entries[0]?.sequence ?? 0,
      toSequence: entries[entries.length - 1]?.sequence ?? 0,
      entries: entries.map((e) => ({
        sequence: e.sequence,
        timestamp: e.timestamp,
        table: e.table,
        operation: e.operation,
        data: e.data ? (JSON.parse(e.data) as Record<string, unknown>) : null,
      })),
    };
  }

  purgeOldSynced(keepLatest = 5000): void {
    const row = this.db
      .prepare(
        `SELECT sequence FROM _driftdb_log WHERE synced = 1 ORDER BY sequence DESC LIMIT 1 OFFSET ?`
      )
      .get(keepLatest) as { sequence: number } | undefined;

    if (row) {
      this.db
        .prepare(`DELETE FROM _driftdb_log WHERE synced = 1 AND sequence <= ?`)
        .run(row.sequence);
    }
  }
}
