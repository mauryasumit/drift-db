import BetterSqlite3 from 'better-sqlite3';
import type Database from 'better-sqlite3';
import { mkdirSync, existsSync, writeFileSync } from 'fs';
import { dirname } from 'path';
import type { DBConfig, ModelSchema, SyncLogEvent, SyncMetrics } from './types.js';
import { Repository } from './orm/repository.js';
import { SyncEngine } from './sync/engine.js';
import { S3Adapter } from './storage/s3-adapter.js';
import { generateNodeId } from './utils/id.js';
import type { Model, ModelStatic } from './orm/model.js';
import type { BaseRecord } from './types.js';

const META_SCHEMA = `
  CREATE TABLE IF NOT EXISTS _driftdb_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
  );
`;

type InternalDBConfig = DBConfig & {
  __skipRestoreGuard?: boolean;
};

export class DB {
  private readonly sqliteDb: Database.Database;
  private readonly config: DBConfig;
  private readonly nodeId: string;
  private readonly syncEngine: SyncEngine;
  private readonly repos = new Map<string, Repository<BaseRecord>>();

  constructor(config: DBConfig) {
    const internalConfig = config as InternalDBConfig;
    const dbName = config.dbName.trim();
    if (!dbName) {
      throw new Error('DBConfig.dbName is required');
    }

    this.config = {
      ...internalConfig,
      dbName,
    };

    if (
      DB.shouldAutoRestore(this.config) &&
      this.config.s3Config &&
      this.config.sqlitePath !== ':memory:' &&
      !existsSync(this.config.sqlitePath) &&
      !internalConfig.__skipRestoreGuard
    ) {
      throw new Error(
        `Local database "${this.config.sqlitePath}" does not exist. Use await DB.open(...) when autoRestore is enabled.`
      );
    }

    if (this.config.sqlitePath !== ':memory:') {
      const dir = dirname(this.config.sqlitePath);
      if (dir && dir !== '.') {
        mkdirSync(dir, { recursive: true });
      }
    }

    this.sqliteDb = new BetterSqlite3(this.config.sqlitePath);
    this.sqliteDb.pragma('journal_mode = WAL');
    this.sqliteDb.pragma('synchronous = NORMAL');
    this.sqliteDb.pragma('foreign_keys = ON');
    this.sqliteDb.pragma('cache_size = -64000');
    this.sqliteDb.pragma('temp_store = MEMORY');

    this.sqliteDb.exec(META_SCHEMA);

    this.nodeId = this.getOrCreateNodeId(this.config.nodeId);
    this.syncEngine = new SyncEngine(this.sqliteDb, this.nodeId, this.config);

    if (this.config.autoSync !== false && this.config.s3Config) {
      this.syncEngine.start();
    }
  }

  /**
   * Async factory — use this instead of `new DB()` when you need S3 restore on startup.
   *
   * - If `autoRestore: true` and the local SQLite file does not exist, it downloads
   *   the latest snapshot from S3 before opening the database.
   * - `dbName` is required and namespaces each logical database in S3.
   * - `nodeId` identifies the current local node within that logical database.
   *
   * @example
   * const db = await DB.open({
   *   dbName: 'my-app-db',
   *   sqlitePath: './data/app.sqlite',
   *   nodeId: 'server-1',
   *   autoRestore: true,          // auto-restore if local file is missing
   *   s3Config: { bucket: '...', region: '...' },
   * });
   */
  static async open(config: DBConfig): Promise<DB> {
    if (
      DB.shouldAutoRestore(config) &&
      config.s3Config &&
      config.sqlitePath !== ':memory:' &&
      !existsSync(config.sqlitePath)
    ) {
      DB.emitLog(config, {
        level: 'info',
        scope: 'restore',
        message: `Local database missing at "${config.sqlitePath}". Attempting restore from S3.`,
        dbName: config.dbName,
      });
      const restored = await DB.restoreSnapshot(config);
      DB.emitLog(config, {
        level: 'info',
        scope: 'restore',
        message: restored
          ? `Restored database "${config.dbName}" from S3 snapshot.`
          : `No remote snapshot found for database "${config.dbName}". Starting with a new local database.`,
        dbName: config.dbName,
      });
    }
    return new DB(
      {
        ...(config as InternalDBConfig),
        __skipRestoreGuard: true,
      } as InternalDBConfig
    );
  }

  private static async restoreSnapshot(config: DBConfig): Promise<boolean> {
    const s3 = new S3Adapter(config.s3Config!);
    const uploadOptions = {
      compress: config.compression !== false,
      encryptionKey: config.encryption?.key,
    };

    const manifest = await s3.getManifest(config.dbName);
    if (!manifest?.latestSnapshotKey) {
      return false;
    }

    const data = await s3.download(manifest.latestSnapshotKey, uploadOptions);
    const dir = dirname(config.sqlitePath);
    if (dir && dir !== '.') {
      mkdirSync(dir, { recursive: true });
    }
    writeFileSync(config.sqlitePath, data);
    return true;
  }

  private static emitLog(config: DBConfig, event: SyncLogEvent): void {
    config.logger?.(event);
  }

  private static shouldAutoRestore(config: DBConfig): boolean {
    return config.autoRestore === true || config.restoreFromS3 === true;
  }

  private getOrCreateNodeId(preferred?: string): string {
    const metaKey = `nodeId:${this.config.dbName}`;

    if (preferred) {
      this.sqliteDb
        .prepare(`INSERT OR REPLACE INTO _driftdb_meta (key, value) VALUES (?, ?)`)
        .run(metaKey, preferred);
      return preferred;
    }

    const row = this.sqliteDb
      .prepare(`SELECT value FROM _driftdb_meta WHERE key = ? OR key = 'nodeId' ORDER BY CASE WHEN key = ? THEN 0 ELSE 1 END LIMIT 1`)
      .get(metaKey, metaKey) as { value: string } | undefined;

    if (row) return row.value;

    const id = generateNodeId();
    this.sqliteDb
      .prepare(`INSERT INTO _driftdb_meta (key, value) VALUES (?, ?)`)
      .run(metaKey, id);
    return id;
  }

  define<S extends ModelSchema>(
    tableName: string,
    schema: S
  ): Repository<BaseRecord & { [K in keyof S]: unknown }> {
    const repo = new Repository<BaseRecord & { [K in keyof S]: unknown }>(
      this.sqliteDb,
      tableName,
      schema,
      this.syncEngine.getChangeLog()
    );
    this.repos.set(tableName, repo as unknown as Repository<BaseRecord>);
    return repo;
  }

  registerModel<T extends Model>(ModelClass: ModelStatic<T>): void {
    if (!ModelClass.tableName) {
      throw new Error(`Model ${ModelClass.name} must define a static 'tableName'`);
    }
    if (!ModelClass.schema) {
      throw new Error(`Model ${ModelClass.name} must define a static 'schema'`);
    }

    const repo = new Repository<T>(
      this.sqliteDb,
      ModelClass.tableName,
      ModelClass.schema,
      this.syncEngine.getChangeLog()
    );

    (ModelClass as unknown as { _repo: Repository<T> })._repo = repo;
    this.repos.set(ModelClass.tableName, repo as unknown as Repository<BaseRecord>);
  }

  getNodeId(): string {
    return this.nodeId;
  }

  getMetrics(): Readonly<SyncMetrics> {
    return this.syncEngine.getMetrics();
  }

  async flush(): Promise<void> {
    return this.syncEngine.flush();
  }

  async snapshot(): Promise<void> {
    return this.syncEngine.triggerSnapshot();
  }

  startSync(): void {
    this.syncEngine.start();
  }

  stopSync(): void {
    this.syncEngine.stop();
  }

  raw(): Database.Database {
    return this.sqliteDb;
  }

  close(): void {
    this.syncEngine.stop();
    this.sqliteDb.close();
  }

  transaction<T>(fn: () => T): T {
    return this.sqliteDb.transaction(fn)();
  }

  vacuum(): void {
    this.sqliteDb.exec('VACUUM');
  }

  integrityCheck(): boolean {
    const result = this.sqliteDb
      .prepare('PRAGMA integrity_check')
      .get() as { integrity_check: string };
    return result.integrity_check === 'ok';
  }
}
