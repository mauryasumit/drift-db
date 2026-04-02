export type ColumnType = 'TEXT' | 'INTEGER' | 'REAL' | 'BLOB' | 'BOOLEAN';

export interface ColumnDef {
  type: ColumnType;
  notNull?: boolean;
  unique?: boolean;
  default?: string | number | boolean | null;
  index?: boolean;
}

export type ModelSchema = Record<string, ColumnDef>;

export interface BaseRecord {
  id: string;
  createdAt: number;
  updatedAt: number;
}

export type WhereValue<V> =
  | V
  | { $gt?: V; $gte?: V; $lt?: V; $lte?: V; $in?: V[]; $like?: string; $ne?: V };

export type WhereClause<T> = {
  [K in keyof T]?: WhereValue<T[K]>;
};

export interface FindOptions<T> {
  where?: WhereClause<T>;
  orderBy?: Partial<Record<keyof T, 'ASC' | 'DESC'>>;
  limit?: number;
  offset?: number;
}

export interface S3Config {
  bucket: string;
  region: string;
  prefix?: string;
  accessKeyId?: string;
  secretAccessKey?: string;
  endpoint?: string;
  forcePathStyle?: boolean;
}

export interface RetryConfig {
  maxRetries: number;
  baseDelayMs: number;
  maxDelayMs: number;
}

export interface EncryptionConfig {
  key: string;
}

export interface DBConfig {
  sqlitePath: string;
  s3Config?: S3Config;
  nodeId?: string;
  syncIntervalMs?: number;
  snapshotEveryNLogs?: number;
  maxBatchSize?: number;
  compression?: boolean;
  encryption?: EncryptionConfig;
  retryConfig?: RetryConfig;
  autoSync?: boolean;
}

export interface ChangeLogEntry {
  sequence: number;
  timestamp: number;
  nodeId: string;
  table: string;
  operation: 'insert' | 'update' | 'delete';
  data: string | null;
  synced: 0 | 1;
}

export interface LogBatch {
  version: 1;
  nodeId: string;
  fromSequence: number;
  toSequence: number;
  entries: Array<{
    sequence: number;
    timestamp: number;
    table: string;
    operation: 'insert' | 'update' | 'delete';
    data: Record<string, unknown> | null;
  }>;
}

export interface SyncJob {
  id: string;
  type: 'upload_log' | 'upload_snapshot';
  payload: string;
  status: 'pending' | 'processing' | 'done' | 'failed';
  attempts: number;
  nextRetryAt: number;
  createdAt: number;
  error: string | null;
}

export interface SyncManifest {
  nodeId: string;
  latestSnapshotKey: string | null;
  latestSnapshotTimestamp: number | null;
  latestLogSequence: number;
  updatedAt: number;
}

export interface SyncMetrics {
  lastSyncAt: number | null;
  lastSnapshotAt: number | null;
  pendingChanges: number;
  dbSizeBytes: number;
  totalSynced: number;
  syncErrors: number;
  isRunning: boolean;
}

export interface UploadLogPayload {
  fromSequence: number;
  toSequence: number;
  s3Key: string;
}

export interface UploadSnapshotPayload {
  timestamp: number;
  s3Key: string;
  dbPath: string;
}
