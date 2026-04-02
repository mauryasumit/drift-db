import { readFileSync, existsSync } from 'fs';
import { join, dirname } from 'path';
import { tmpdir } from 'os';
import { mkdirSync, copyFileSync, unlinkSync } from 'fs';
import type Database from 'better-sqlite3';
import type { S3Adapter } from '../storage/s3-adapter.js';
import type { S3UploadOptions } from '../storage/s3-adapter.js';

export class SnapshotManager {
  private readonly db: Database.Database;
  private readonly s3: S3Adapter;
  private readonly dbName: string;
  private readonly nodeId: string;
  private readonly sqlitePath: string;
  private readonly uploadOptions: S3UploadOptions;

  constructor(
    db: Database.Database,
    s3: S3Adapter,
    dbName: string,
    nodeId: string,
    sqlitePath: string,
    uploadOptions: S3UploadOptions
  ) {
    this.db = db;
    this.s3 = s3;
    this.dbName = dbName;
    this.nodeId = nodeId;
    this.sqlitePath = sqlitePath;
    this.uploadOptions = uploadOptions;
  }

  async takeAndUpload(): Promise<{ key: string; timestamp: number }> {
    const timestamp = Date.now();
    const tempPath = join(tmpdir(), `driftdb-snap-${this.nodeId}-${timestamp}.sqlite`);

    try {
      this.db.exec('PRAGMA wal_checkpoint(FULL)');

      if (this.sqlitePath === ':memory:') {
        const backup = this.db.serialize();
        await this.s3.upload(
          this.s3.snapshotKey(this.dbName, this.nodeId, timestamp),
          Buffer.from(backup),
          this.uploadOptions
        );
      } else {
        copyFileSync(this.sqlitePath, tempPath);
        const data = readFileSync(tempPath);
        await this.s3.upload(
          this.s3.snapshotKey(this.dbName, this.nodeId, timestamp),
          data,
          this.uploadOptions
        );
      }

      const key = this.s3.snapshotKey(this.dbName, this.nodeId, timestamp);
      return { key, timestamp };
    } finally {
      if (existsSync(tempPath)) {
        try { unlinkSync(tempPath); } catch { /* ignore */ }
      }
    }
  }

  async restoreLatest(): Promise<boolean> {
    const manifest = await this.s3.getManifest(this.dbName);
    if (!manifest?.latestSnapshotKey) return false;

    const data = await this.s3.download(manifest.latestSnapshotKey, this.uploadOptions);

    const restorePath = this.sqlitePath !== ':memory:' ? this.sqlitePath : null;
    if (!restorePath) return false;

    const dir = dirname(restorePath);
    mkdirSync(dir, { recursive: true });

    const { writeFileSync } = await import('fs');
    writeFileSync(restorePath, data);

    return true;
  }
}
