import BetterSqlite3 from 'better-sqlite3';
import { ChangeLog } from '../src/sync/change-log';
import { DB } from '../src/db';

describe('ChangeLog', () => {
  function makeChangeLog() {
    const db = new BetterSqlite3(':memory:');
    const log = new ChangeLog(db, 'test-node');
    return { db, log };
  }

  test('append and pendingEntries', () => {
    const { log, db } = makeChangeLog();

    log.append('users', 'insert', { id: '1', name: 'Alice' });
    log.append('users', 'update', { where: { id: '1' }, data: { name: 'Alice B' } });

    const pending = log.pendingEntries(10);
    expect(pending).toHaveLength(2);
    expect(pending[0]?.operation).toBe('insert');
    expect(pending[1]?.operation).toBe('update');
    db.close();
  });

  test('markSynced removes from pending', () => {
    const { log, db } = makeChangeLog();

    log.append('users', 'insert', { id: '1' });
    log.append('users', 'insert', { id: '2' });

    const pending = log.pendingEntries(10);
    expect(pending).toHaveLength(2);

    log.markSynced(pending[0]!.sequence, pending[1]!.sequence);

    const afterSync = log.pendingEntries(10);
    expect(afterSync).toHaveLength(0);
    db.close();
  });

  test('pendingCount', () => {
    const { log, db } = makeChangeLog();

    expect(log.pendingCount()).toBe(0);
    log.append('items', 'insert', { id: 'x' });
    expect(log.pendingCount()).toBe(1);
    db.close();
  });

  test('buildBatch produces correct structure', () => {
    const { log, db } = makeChangeLog();

    log.append('users', 'insert', { id: '1', name: 'Alice' });
    log.append('users', 'delete', { id: '1' });

    const entries = log.pendingEntries(10);
    const batch = log.buildBatch(entries);

    expect(batch.version).toBe(1);
    expect(batch.nodeId).toBe('test-node');
    expect(batch.entries).toHaveLength(2);
    expect(batch.fromSequence).toBe(entries[0]?.sequence);
    expect(batch.toSequence).toBe(entries[entries.length - 1]?.sequence);
    db.close();
  });

  test('latestSyncedSequence', () => {
    const { log, db } = makeChangeLog();

    expect(log.latestSyncedSequence()).toBe(0);

    log.append('t', 'insert', { id: '1' });
    log.append('t', 'insert', { id: '2' });
    const entries = log.pendingEntries(10);
    log.markSynced(entries[0]!.sequence, entries[1]!.sequence);

    expect(log.latestSyncedSequence()).toBeGreaterThan(0);
    db.close();
  });

  test('purgeOldSynced keeps recent entries', () => {
    const { log, db } = makeChangeLog();

    for (let i = 0; i < 10; i++) {
      log.append('t', 'insert', { id: String(i) });
    }
    const all = log.pendingEntries(100);
    log.markSynced(all[0]!.sequence, all[all.length - 1]!.sequence);

    log.purgeOldSynced(5);

    const remaining = db.prepare(`SELECT COUNT(*) as cnt FROM _driftdb_log WHERE synced = 1`).get() as { cnt: number };
    expect(remaining.cnt).toBeLessThanOrEqual(5);
    db.close();
  });
});

describe('SyncEngine - no S3', () => {
  test('getMetrics returns correct structure', () => {
    const db = new DB({ sqlitePath: ':memory:', autoSync: false });
    const metrics = db.getMetrics();

    expect(metrics).toHaveProperty('isRunning');
    expect(metrics).toHaveProperty('pendingChanges');
    expect(metrics).toHaveProperty('totalSynced');
    expect(metrics.isRunning).toBe(false);
    expect(metrics.pendingChanges).toBe(0);
    db.close();
  });

  test('flush is a no-op when no S3 configured', async () => {
    const db = new DB({ sqlitePath: ':memory:', autoSync: false });

    await expect(db.flush()).resolves.not.toThrow();
    db.close();
  });

  test('changes get logged in change log', async () => {
    const db = new DB({ sqlitePath: ':memory:', autoSync: false });

    const Users = db.define('users', { name: { type: 'TEXT' } });
    await Users.create({ name: 'Test' });

    const metrics = db.getMetrics();
    expect(metrics.pendingChanges).toBe(1);
    db.close();
  });
});
