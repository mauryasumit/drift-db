import BetterSqlite3 from 'better-sqlite3';
import { SyncQueue } from '../src/queue/queue';
import type { RetryConfig } from '../src/types';

const retryConfig: RetryConfig = { maxRetries: 3, baseDelayMs: 100, maxDelayMs: 5000 };

function makeDb() {
  return new BetterSqlite3(':memory:');
}

describe('SyncQueue', () => {
  test('enqueue and dequeue', () => {
    const db = makeDb();
    const queue = new SyncQueue(db, retryConfig);

    queue.enqueue('upload_log', { fromSequence: 1, toSequence: 10 });
    queue.enqueue('upload_log', { fromSequence: 11, toSequence: 20 });

    const jobs = queue.dequeue(5);
    expect(jobs).toHaveLength(2);
    expect(jobs[0]?.type).toBe('upload_log');
    expect(jobs[0]?.status).toBe('processing');
    db.close();
  });

  test('markDone removes from active queue', () => {
    const db = makeDb();
    const queue = new SyncQueue(db, retryConfig);

    queue.enqueue('upload_snapshot', { timestamp: Date.now() });
    const jobs = queue.dequeue(1);
    expect(jobs).toHaveLength(1);

    queue.markDone(jobs[0]!.id);

    const again = queue.dequeue(1);
    expect(again).toHaveLength(0);
    db.close();
  });

  test('markFailed increments attempts', () => {
    const db = makeDb();
    const queue = new SyncQueue(db, retryConfig);

    queue.enqueue('upload_log', { fromSequence: 1, toSequence: 5 });
    const jobs = queue.dequeue(1);

    queue.markFailed(jobs[0]!.id, 'Network error');

    const pending = queue.pendingCount();
    expect(pending).toBe(1);
    db.close();
  });

  test('pendingCount is accurate', () => {
    const db = makeDb();
    const queue = new SyncQueue(db, retryConfig);

    expect(queue.pendingCount()).toBe(0);

    queue.enqueue('upload_log', { x: 1 });
    queue.enqueue('upload_log', { x: 2 });
    expect(queue.pendingCount()).toBe(2);

    const jobs = queue.dequeue(1);
    queue.markDone(jobs[0]!.id);
    expect(queue.pendingCount()).toBe(1);
    db.close();
  });

  test('hasPendingOfType', () => {
    const db = makeDb();
    const queue = new SyncQueue(db, retryConfig);

    expect(queue.hasPendingOfType('upload_log')).toBe(false);
    queue.enqueue('upload_log', { x: 1 });
    expect(queue.hasPendingOfType('upload_log')).toBe(true);
    expect(queue.hasPendingOfType('upload_snapshot')).toBe(false);
    db.close();
  });

  test('resetStuck requeues processing jobs from long ago', () => {
    const db = makeDb();
    const queue = new SyncQueue(db, retryConfig);

    queue.enqueue('upload_log', { x: 1 });
    const jobs = queue.dequeue(1);
    expect(jobs[0]?.status).toBe('processing');

    db.prepare(
      `UPDATE _driftdb_queue SET createdAt = ? WHERE id = ?`
    ).run(Date.now() - 10 * 60 * 1000, jobs[0]!.id);

    queue.resetStuck();

    const requeued = queue.dequeue(1);
    expect(requeued).toHaveLength(1);
    db.close();
  });

  test('purgeCompleted removes old done jobs', () => {
    const db = makeDb();
    const queue = new SyncQueue(db, retryConfig);

    queue.enqueue('upload_log', { x: 1 });
    const jobs = queue.dequeue(1);
    queue.markDone(jobs[0]!.id);

    db.prepare(
      `UPDATE _driftdb_queue SET createdAt = ? WHERE id = ?`
    ).run(Date.now() - 25 * 60 * 60 * 1000, jobs[0]!.id);

    queue.purgeCompleted(24 * 60 * 60 * 1000);

    const count = db.prepare(`SELECT COUNT(*) as cnt FROM _driftdb_queue`).get() as { cnt: number };
    expect(count.cnt).toBe(0);
    db.close();
  });
});
