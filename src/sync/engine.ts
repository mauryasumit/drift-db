import type Database from 'better-sqlite3';
import type { DBConfig, SyncJob, SyncMetrics, UploadLogPayload, UploadSnapshotPayload } from '../types.js';
import { SyncQueue } from '../queue/queue.js';
import { ChangeLog } from './change-log.js';
import { SnapshotManager } from './snapshot-manager.js';
import { S3Adapter } from '../storage/s3-adapter.js';
import { withRetry } from '../utils/retry.js';
import { statSync, existsSync } from 'fs';

const DEFAULT_SYNC_INTERVAL_MS = 5_000;
const DEFAULT_SNAPSHOT_EVERY_N_LOGS = 1_000;
const DEFAULT_MAX_BATCH_SIZE = 100;
const DEFAULT_RETRY_CONFIG = { maxRetries: 5, baseDelayMs: 500, maxDelayMs: 30_000 };

export class SyncEngine {
  private readonly db: Database.Database;
  private readonly config: DBConfig;
  private readonly nodeId: string;
  private readonly queue: SyncQueue;
  private readonly changeLog: ChangeLog;
  private readonly snapshotManager: SnapshotManager | null;
  private readonly s3: S3Adapter | null;

  private timer: ReturnType<typeof setInterval> | null = null;
  private isProcessing = false;

  private metrics: SyncMetrics = {
    lastSyncAt: null,
    lastSnapshotAt: null,
    pendingChanges: 0,
    dbSizeBytes: 0,
    totalSynced: 0,
    syncErrors: 0,
    isRunning: false,
  };

  constructor(db: Database.Database, nodeId: string, config: DBConfig) {
    this.db = db;
    this.config = config;
    this.nodeId = nodeId;

    const retryConfig = { ...DEFAULT_RETRY_CONFIG, ...config.retryConfig };
    this.queue = new SyncQueue(db, retryConfig);
    this.changeLog = new ChangeLog(db, nodeId);

    if (config.s3Config) {
      this.s3 = new S3Adapter(config.s3Config);
      const uploadOptions = {
        compress: config.compression !== false,
        encryptionKey: config.encryption?.key,
      };
      this.snapshotManager = new SnapshotManager(
        db,
        this.s3,
        nodeId,
        config.sqlitePath,
        uploadOptions
      );
    } else {
      this.s3 = null;
      this.snapshotManager = null;
    }
  }

  getChangeLog(): ChangeLog {
    return this.changeLog;
  }

  getQueue(): SyncQueue {
    return this.queue;
  }

  start(): void {
    if (this.timer) return;
    this.metrics.isRunning = true;
    this.queue.resetStuck();

    const intervalMs = this.config.syncIntervalMs ?? DEFAULT_SYNC_INTERVAL_MS;
    this.timer = setInterval(() => {
      void this.tick();
    }, intervalMs);

    if (this.timer.unref) {
      this.timer.unref();
    }
  }

  stop(): void {
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }
    this.metrics.isRunning = false;
  }

  async flush(): Promise<void> {
    await this.tick();
  }

  getMetrics(): Readonly<SyncMetrics> {
    return {
      ...this.metrics,
      pendingChanges: this.changeLog.pendingCount(),
      dbSizeBytes: this.getDbSizeBytes(),
    };
  }

  private getDbSizeBytes(): number {
    const path = this.config.sqlitePath;
    if (path === ':memory:' || !existsSync(path)) return 0;
    try {
      return statSync(path).size;
    } catch {
      return 0;
    }
  }

  private async tick(): Promise<void> {
    if (this.isProcessing || !this.s3) return;
    this.isProcessing = true;

    try {
      await this.enqueuePendingLogs();
      await this.processQueue();
    } catch (err) {
      this.metrics.syncErrors++;
    } finally {
      this.isProcessing = false;
    }
  }

  private async enqueuePendingLogs(): Promise<void> {
    const maxBatch = this.config.maxBatchSize ?? DEFAULT_MAX_BATCH_SIZE;
    const pending = this.changeLog.pendingEntries(maxBatch);
    if (pending.length === 0) return;

    const batch = this.changeLog.buildBatch(pending);
    const s3Key = this.s3!.logKey(this.nodeId, batch.fromSequence, batch.toSequence);

    const alreadyQueued = this.queue.hasPendingOfType('upload_log');
    if (!alreadyQueued) {
      const payload: UploadLogPayload = {
        fromSequence: batch.fromSequence,
        toSequence: batch.toSequence,
        s3Key,
      };
      this.queue.enqueue('upload_log', { ...payload, batch });
    }
  }

  private async processQueue(): Promise<void> {
    const jobs = this.queue.dequeue(3);

    await Promise.allSettled(
      jobs.map((job) => this.processJob(job))
    );

    this.queue.purgeCompleted();
  }

  private async processJob(job: SyncJob): Promise<void> {
    const retryConfig = { ...DEFAULT_RETRY_CONFIG, ...this.config.retryConfig };
    const uploadOptions = {
      compress: this.config.compression !== false,
      encryptionKey: this.config.encryption?.key,
    };

    try {
      await withRetry(
        async () => {
          if (job.type === 'upload_log') {
            const p = JSON.parse(job.payload) as UploadLogPayload & { batch: unknown };
            const batchBuffer = Buffer.from(JSON.stringify(p.batch), 'utf8');
            await this.s3!.upload(p.s3Key, batchBuffer, uploadOptions);
            this.changeLog.markSynced(p.fromSequence, p.toSequence);

            const manifest = await this.s3!.getManifest(this.nodeId);
            const latestSeq = Math.max(
              manifest?.latestLogSequence ?? 0,
              p.toSequence
            );
            await this.s3!.putManifest(this.nodeId, {
              nodeId: this.nodeId,
              latestSnapshotKey: manifest?.latestSnapshotKey ?? null,
              latestSnapshotTimestamp: manifest?.latestSnapshotTimestamp ?? null,
              latestLogSequence: latestSeq,
              updatedAt: Date.now(),
            });

            this.metrics.totalSynced += (p.toSequence - p.fromSequence + 1);
            this.metrics.lastSyncAt = Date.now();

            await this.maybeSnapshot(latestSeq);
          } else if (job.type === 'upload_snapshot') {
            const p = JSON.parse(job.payload) as UploadSnapshotPayload;
            if (this.snapshotManager) {
              const { key, timestamp } = await this.snapshotManager.takeAndUpload();
              const manifest = await this.s3!.getManifest(this.nodeId);
              await this.s3!.putManifest(this.nodeId, {
                nodeId: this.nodeId,
                latestSnapshotKey: key,
                latestSnapshotTimestamp: timestamp,
                latestLogSequence: manifest?.latestLogSequence ?? 0,
                updatedAt: Date.now(),
              });
              this.metrics.lastSnapshotAt = Date.now();
            }
            void p;
          }
        },
        retryConfig
      );

      this.queue.markDone(job.id);
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      this.queue.markFailed(job.id, msg);
      this.metrics.syncErrors++;
    }
  }

  private async maybeSnapshot(latestSequence: number): Promise<void> {
    const threshold = this.config.snapshotEveryNLogs ?? DEFAULT_SNAPSHOT_EVERY_N_LOGS;
    if (latestSequence > 0 && latestSequence % threshold === 0) {
      if (!this.queue.hasPendingOfType('upload_snapshot')) {
        const payload: UploadSnapshotPayload = {
          timestamp: Date.now(),
          s3Key: this.s3!.snapshotKey(this.nodeId, Date.now()),
          dbPath: this.config.sqlitePath,
        };
        this.queue.enqueue('upload_snapshot', payload);
      }
    }
  }

  async triggerSnapshot(): Promise<void> {
    if (!this.snapshotManager || !this.s3) return;
    const { key, timestamp } = await this.snapshotManager.takeAndUpload();
    const manifest = await this.s3.getManifest(this.nodeId);
    await this.s3.putManifest(this.nodeId, {
      nodeId: this.nodeId,
      latestSnapshotKey: key,
      latestSnapshotTimestamp: timestamp,
      latestLogSequence: manifest?.latestLogSequence ?? 0,
      updatedAt: Date.now(),
    });
    this.metrics.lastSnapshotAt = Date.now();
  }
}
