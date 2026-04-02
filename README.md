# 🌊 DriftDB

**Local-first SQLite database with automatic S3 sync.**

DriftDB keeps all reads and writes local (fast, offline-capable), and automatically drifts your changes to Amazon S3 in the background — no Redis, no Kafka, no external infrastructure required.

---

## Table of Contents

- [Why DriftDB?](#why-driftdb)
- [Architecture Overview](#architecture-overview)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [API Reference](#api-reference)
  - [DB](#db)
  - [Repository (schema-based API)](#repository-schema-based-api)
  - [Model (class-based API)](#model-class-based-api)
  - [Column Builder](#column-builder)
- [S3 Sync Engine](#s3-sync-engine)
  - [How Sync Works](#how-sync-works)
  - [S3 Layout](#s3-layout)
  - [Snapshots](#snapshots)
- [Compression & Encryption](#compression--encryption)
- [Failure Recovery](#failure-recovery)
- [Performance](#performance)
- [Configuration Reference](#configuration-reference)
- [Advanced Usage](#advanced-usage)
- [Trade-offs & Design Decisions](#trade-offs--design-decisions)

---

## Why DriftDB?

| Feature | DriftDB | Traditional DB |
|---|---|---|
| Offline reads/writes | ✅ Always works | ❌ Network required |
| Zero infrastructure | ✅ Only S3 | ❌ Servers, clusters |
| Local-first speed | ✅ SQLite speed | ❌ Network latency |
| Durable replication | ✅ S3 backed | ✅ |
| Type-safe ORM | ✅ | Varies |
| Crash recovery | ✅ Idempotent sync | Varies |

DriftDB is ideal for:
- **CLI tools** that need persistent local state with cloud backup
- **Edge / embedded applications** that must work offline
- **Single-tenant SaaS** where each customer gets their own isolated SQLite+S3 pair
- **Developer tools, agents, pipelines** that need durable local storage without an ops burden

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────┐
│                   Your Application                  │
│                                                     │
│   const Users = db.define('users', schema)          │
│   await Users.create({ name: 'Alice' })  ◄── fast   │
│   await Users.filter({ age: { $gte: 18 } })         │
└──────────────────────────┬──────────────────────────┘
                           │ all reads/writes local
                           ▼
┌─────────────────────────────────────────────────────┐
│               SQLite (WAL mode)                     │
│                                                     │
│  ┌──────────────┐  ┌──────────────┐                 │
│  │  Your tables │  │ _driftdb_log │  change log     │
│  │  (users, ...) │  │ _driftdb_queue│  sync queue   │
│  └──────────────┘  └──────────────┘                 │
└──────────────────────────┬──────────────────────────┘
                           │ async, non-blocking
                           ▼
┌─────────────────────────────────────────────────────┐
│              Background Sync Engine                 │
│                                                     │
│  • Batches pending log entries                      │
│  • Uploads compressed JSON logs to S3               │
│  • Periodic full snapshots                          │
│  • Retry with exponential backoff                   │
└──────────────────────────┬──────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────┐
│                  Amazon S3                          │
│                                                     │
│  nodes/{nodeId}/logs/000000000001-000000000100.json │
│  nodes/{nodeId}/snapshots/1704067200000.sqlite      │
│  nodes/{nodeId}/manifest.json                       │
└─────────────────────────────────────────────────────┘
```

---

## Installation

```bash
npm install driftdb
```

**Requirements:**
- Node.js >= 18
- An S3-compatible bucket (AWS S3, MinIO, LocalStack, etc.)

---

## Quick Start

### Schema-based API (recommended)

```typescript
import { DB, Column } from 'driftdb';

const db = new DB({
  sqlitePath: './data/myapp.sqlite',
  s3Config: {
    bucket: 'my-app-backups',
    region: 'us-east-1',
  },
});

// Define a model
const Users = db.define('users', {
  name:  Column.text().required().build(),
  email: Column.text().unique().required().build(),
  age:   Column.integer().build(),
});

// Create
const alice = await Users.create({ name: 'Alice', email: 'alice@example.com', age: 30 });

// Query
const adults = await Users.find({
  where:   { age: { $gte: 18 } },
  orderBy: { name: 'ASC' },
  limit:   10,
});

// Update
await Users.update({ id: alice.id }, { name: 'Alice Smith' });

// Delete
await Users.delete({ id: alice.id });

// Metrics
console.log(db.getMetrics());

db.close();
```

### Class-based API

```typescript
import { DB, Model, Column } from 'driftdb';
import type { ModelSchema } from 'driftdb';

const db = new DB({ sqlitePath: './data.sqlite' });

class User extends Model {
  static tableName = 'users';
  static schema: ModelSchema = {
    name:  Column.text().required().build(),
    email: Column.text().unique().build(),
  };

  name!: string;
  email!: string;
}

db.registerModel(User);

const alice = await User.create({ name: 'Alice', email: 'alice@example.com' });
const users = await User.filter({ name: 'Alice' });
await User.update({ id: alice.id }, { name: 'Alice Smith' });
```

---

## API Reference

### DB

The main entry point. Manages the SQLite connection, ORM registrations, and sync engine.

```typescript
const db = new DB(config: DBConfig);
```

| Method | Description |
|---|---|
| `db.define(tableName, schema)` | Create a typed `Repository` for a table |
| `db.registerModel(ModelClass)` | Register a class-based `Model` |
| `db.getMetrics()` | Returns `SyncMetrics` |
| `db.flush()` | Force an immediate sync tick |
| `db.snapshot()` | Manually trigger a full DB snapshot upload |
| `db.startSync()` | Start background sync (auto-started if `s3Config` provided) |
| `db.stopSync()` | Stop background sync |
| `db.transaction(fn)` | Run `fn` in a SQLite transaction |
| `db.integrityCheck()` | Returns `true` if the DB passes `PRAGMA integrity_check` |
| `db.vacuum()` | Run `VACUUM` to reclaim space |
| `db.raw()` | Access the raw `better-sqlite3` Database instance |
| `db.getNodeId()` | Returns the unique node ID for this instance |
| `db.close()` | Stop sync and close the SQLite connection |

---

### Repository (schema-based API)

Returned by `db.define(tableName, schema)`.

```typescript
const Users = db.define('users', {
  name:  { type: 'TEXT', notNull: true },
  email: { type: 'TEXT', unique: true },
  age:   { type: 'INTEGER' },
});
```

Every record automatically gets these base fields:

| Field | Type | Description |
|---|---|---|
| `id` | `string` | UUID primary key |
| `createdAt` | `number` | Unix timestamp (ms) |
| `updatedAt` | `number` | Unix timestamp (ms), auto-updated |

#### `create(data)`

```typescript
const user = await Users.create({ name: 'Alice', email: 'alice@example.com' });
// Returns the full record including id, createdAt, updatedAt
```

#### `findById(id)`

```typescript
const user = await Users.findById('some-uuid');
// Returns record or null
```

#### `findOne(where)`

```typescript
const user = await Users.findOne({ email: 'alice@example.com' });
```

#### `find(options)`

```typescript
const users = await Users.find({
  where:   { age: { $gte: 18 } },
  orderBy: { name: 'ASC' },
  limit:   20,
  offset:  0,
});
```

#### `filter(where, options?)`

Shorthand for `find` with just a `where` clause:

```typescript
const active = await Users.filter({ active: 1 });
```

#### `update(where, data)`

```typescript
const affected = await Users.update({ id: user.id }, { name: 'Alice Smith' });
// Returns number of rows updated
```

#### `delete(where)`

```typescript
const deleted = await Users.delete({ email: 'alice@example.com' });
// Returns number of rows deleted
```

#### `deleteById(id)`

```typescript
const ok = await Users.deleteById('some-uuid');
// Returns boolean
```

#### `count(where?)`

```typescript
const total = await Users.count();
const adults = await Users.count({ age: { $gte: 18 } });
```

#### `upsert(where, data)`

Creates a record if none matches `where`, otherwise updates:

```typescript
const user = await Users.upsert(
  { email: 'alice@example.com' },
  { name: 'Alice', email: 'alice@example.com' }
);
```

#### `raw<R>(sql, params?)`

Execute arbitrary SQL and return typed results:

```typescript
const rows = Users.raw<{ name: string; count: number }>(
  'SELECT name, COUNT(*) as count FROM users GROUP BY name'
);
```

---

### Where Clause Operators

All `where` arguments support the following operators:

```typescript
// Equality
{ name: 'Alice' }

// Comparison
{ age: { $gt: 18 } }
{ age: { $gte: 18 } }
{ age: { $lt: 65 } }
{ age: { $lte: 65 } }

// Not equal
{ status: { $ne: 'deleted' } }

// IN list
{ role: { $in: ['admin', 'moderator'] } }

// LIKE pattern
{ name: { $like: 'Ali%' } }

// NULL check
{ deletedAt: null }

// Combine multiple fields (implicit AND)
{ age: { $gte: 18 }, active: 1 }
```

---

### Model (class-based API)

Extend `Model` to define a model with static CRUD methods. You must define `tableName` and `schema` as static properties.

```typescript
class Post extends Model {
  static tableName = 'posts';
  static schema: ModelSchema = {
    title:     Column.text().required().build(),
    content:   Column.text().build(),
    published: Column.boolean().default(false).build(),
    views:     Column.integer().default(0).build(),
  };

  title!: string;
  content!: string;
  published!: boolean;
  views!: number;
}

db.registerModel(Post);
```

All `Repository` methods are available as static methods on the class:

```typescript
await Post.create({ title: 'Hello', content: '...' });
await Post.findById(id);
await Post.findOne({ title: 'Hello' });
await Post.find({ where: { published: true }, orderBy: { views: 'DESC' } });
await Post.filter({ published: true });
await Post.update({ id }, { views: 100 });
await Post.delete({ id });
await Post.count();
await Post.upsert({ title: 'Hello' }, { title: 'Hello', views: 0 });
```

---

### Column Builder

The `Column` builder provides a fluent API for defining schemas:

```typescript
import { Column } from 'driftdb';

const schema = {
  name:      Column.text().required().build(),
  email:     Column.text().unique().required().build(),
  score:     Column.real().default(0).build(),
  active:    Column.boolean().default(true).build(),
  data:      Column.blob().build(),
  createdBy: Column.integer().index().build(),
};
```

| Builder | SQLite Type | Description |
|---|---|---|
| `Column.text()` | `TEXT` | UTF-8 string |
| `Column.integer()` | `INTEGER` | 64-bit integer |
| `Column.real()` | `REAL` | 64-bit float |
| `Column.boolean()` | `BOOLEAN` | Stored as 0/1 |
| `Column.blob()` | `BLOB` | Raw binary |

| Modifier | Description |
|---|---|
| `.required()` | Adds `NOT NULL` constraint |
| `.unique()` | Adds `UNIQUE` constraint |
| `.default(value)` | Adds `DEFAULT value` |
| `.index()` | Creates an index on this column |
| `.build()` | Returns the `ColumnDef` object |

---

## S3 Sync Engine

### How Sync Works

1. **Every write** to a repository appends an entry to the `_driftdb_log` table in SQLite. This is synchronous and local — zero network cost.

2. **Every `syncIntervalMs`** (default: 5 seconds), the sync engine:
   - Reads pending (un-synced) log entries up to `maxBatchSize`
   - Serializes them into a compressed (optionally encrypted) JSON batch
   - Uploads the batch to S3
   - Marks those entries as synced in SQLite
   - Updates the node's `manifest.json` on S3
   - If `latestSequence % snapshotEveryNLogs === 0`, enqueues a snapshot job

3. **On crash/restart**, the sync engine calls `resetStuck()` on the queue, which re-queues any jobs that were "processing" when the process died. Since S3 keys are deterministic, re-uploading is safe.

### S3 Layout

```
{prefix}/
  nodes/{nodeId}/
    logs/
      000000000001-000000000100.json      ← batch: sequences 1–100
      000000000101-000000000200.json      ← batch: sequences 101–200
    snapshots/
      1704067200000.sqlite                ← full snapshot at this timestamp
    manifest.json                         ← pointer to latest snapshot + sequence
```

**`manifest.json` example:**
```json
{
  "nodeId": "a1b2c3d4e5f6",
  "latestSnapshotKey": "nodes/a1b2c3/snapshots/1704067200000.sqlite",
  "latestSnapshotTimestamp": 1704067200000,
  "latestLogSequence": 1500,
  "updatedAt": 1704070800000
}
```

**Log batch example:**
```json
{
  "version": 1,
  "nodeId": "a1b2c3d4e5f6",
  "fromSequence": 1,
  "toSequence": 100,
  "entries": [
    {
      "sequence": 1,
      "timestamp": 1704067200000,
      "table": "users",
      "operation": "insert",
      "data": { "id": "uuid-...", "name": "Alice", "email": "alice@example.com" }
    },
    {
      "sequence": 2,
      "timestamp": 1704067201000,
      "table": "users",
      "operation": "update",
      "data": { "where": { "id": "uuid-..." }, "data": { "name": "Alice Smith" } }
    }
  ]
}
```

### Snapshots

A snapshot is a full copy of the SQLite database file uploaded to S3. Snapshots serve as recovery checkpoints — on a fresh node, you download the latest snapshot then apply any log entries after it.

Snapshots are triggered:
1. **Automatically** every `snapshotEveryNLogs` log entries (default: 1000)
2. **Manually** via `await db.snapshot()`

Before taking a snapshot, DriftDB runs `PRAGMA wal_checkpoint(FULL)` to flush the WAL and ensure a consistent file.

---

## Compression & Encryption

Both are optional and apply to S3 uploads only. Local SQLite is never compressed or encrypted by DriftDB.

### Compression

Enabled by default (`compression: true`). Uses Node.js built-in `zlib` (gzip, `Z_BEST_SPEED`). Typical JSON log batches compress 60–80%.

```typescript
const db = new DB({
  sqlitePath: './data.sqlite',
  s3Config: { bucket: 'my-bucket', region: 'us-east-1' },
  compression: true,   // default: true
});
```

### Encryption

Uses AES-256-GCM via Node.js built-in `crypto`. Encryption happens **after** compression. Each upload uses a fresh random IV, and the auth tag is prepended to the ciphertext.

```typescript
const db = new DB({
  sqlitePath: './data.sqlite',
  s3Config: { bucket: 'my-bucket', region: 'us-east-1' },
  encryption: {
    key: 'a'.repeat(64),  // 64 hex chars = 32 bytes = AES-256
  },
});
```

Generate a secure key:
```bash
node -e "console.log(require('crypto').randomBytes(32).toString('hex'))"
```

> **Important:** Store the key securely (e.g., AWS Secrets Manager, environment variable). Losing the key means losing access to all encrypted uploads.

---

## Failure Recovery

DriftDB is designed to survive crashes, network failures, and partial uploads.

### Crash during upload

The sync job stays in the `_driftdb_queue` table with status `processing`. On restart, `resetStuck()` re-queues it as `pending`. The upload is retried from scratch. Since S3 keys are deterministic (based on sequence range), re-uploading the same batch is a safe no-op.

### Network failure

Failed jobs are retried with **exponential backoff + jitter**:

```
attempt 1: 500ms delay
attempt 2: 1000ms delay
attempt 3: 2000ms delay
attempt 4: 4000ms delay
attempt 5: 8000ms delay (capped at maxDelayMs)
```

Configure via `retryConfig`:
```typescript
const db = new DB({
  retryConfig: {
    maxRetries: 5,
    baseDelayMs: 500,
    maxDelayMs: 30_000,
  },
});
```

### SQLite corruption prevention

- **WAL mode** is always enabled (`PRAGMA journal_mode = WAL`) — writes are safe from corruption even if the process is killed mid-write
- Before snapshots, `PRAGMA wal_checkpoint(FULL)` ensures a consistent file is captured
- `db.integrityCheck()` runs `PRAGMA integrity_check` and returns `true` if the DB is healthy

### Verifying DB health on startup

```typescript
const db = new DB({ sqlitePath: './data.sqlite' });

if (!db.integrityCheck()) {
  console.error('DB corrupted — restore from S3 snapshot');
  process.exit(1);
}
```

---

## Performance

### Local operations

DriftDB uses `better-sqlite3`, which is **synchronous** and consistently one of the fastest SQLite drivers for Node.js. Typical benchmarks:

| Operation | Speed |
|---|---|
| Single insert | ~50,000 ops/sec |
| Bulk insert (transaction) | ~500,000 rows/sec |
| Point lookup by id | ~100,000 ops/sec |
| Range query (indexed) | ~50,000 rows/sec |

### Sync performance

- Log entries are **batched** — a single S3 `PutObject` covers up to `maxBatchSize` (default: 100) operations
- S3 uploads never block the main thread — they run in the Node.js async I/O pool
- The sync timer calls `timer.unref()` so it does not keep the process alive after your app logic finishes

### Bulk inserts

Wrap bulk inserts in a transaction to avoid per-row WAL flushes:

```typescript
db.transaction(() => {
  for (const item of largeArray) {
    Items.create(item);  // sync internally
  }
});
```

### SQLite pragmas applied by default

| Pragma | Value | Reason |
|---|---|---|
| `journal_mode` | `WAL` | Non-blocking reads during writes |
| `synchronous` | `NORMAL` | Fast without sacrificing crash safety |
| `foreign_keys` | `ON` | Referential integrity |
| `cache_size` | `-64000` | 64MB page cache |
| `temp_store` | `MEMORY` | Temp tables in RAM |

---

## Configuration Reference

```typescript
interface DBConfig {
  sqlitePath: string;          // Path to SQLite file, or ':memory:'

  s3Config?: {
    bucket: string;            // S3 bucket name
    region: string;            // AWS region
    prefix?: string;           // Key prefix (default: 'driftdb')
    accessKeyId?: string;      // AWS credentials (optional if using IAM role)
    secretAccessKey?: string;
    endpoint?: string;         // Custom endpoint for MinIO/LocalStack
    forcePathStyle?: boolean;  // Required for MinIO (default: true if endpoint set)
  };

  nodeId?: string;             // Stable ID for this instance (auto-generated if omitted)
  autoSync?: boolean;          // Start sync automatically (default: true if s3Config set)
  syncIntervalMs?: number;     // How often to sync (default: 5000ms)
  snapshotEveryNLogs?: number; // Take snapshot every N log entries (default: 1000)
  maxBatchSize?: number;       // Max log entries per S3 upload (default: 100)
  compression?: boolean;       // gzip compress uploads (default: true)

  encryption?: {
    key: string;               // 64-char hex = 32-byte AES-256 key
  };

  retryConfig?: {
    maxRetries: number;        // Max retry attempts (default: 5)
    baseDelayMs: number;       // Base delay for exponential backoff (default: 500)
    maxDelayMs: number;        // Max delay cap (default: 30000)
  };
}
```

---

## Advanced Usage

### Using with LocalStack / MinIO

```typescript
const db = new DB({
  sqlitePath: './local.sqlite',
  s3Config: {
    bucket: 'local-test',
    region: 'us-east-1',
    endpoint: 'http://localhost:4566',   // LocalStack
    forcePathStyle: true,
    accessKeyId: 'test',
    secretAccessKey: 'test',
  },
});
```

### Manual sync control

Disable auto-sync and flush on demand:

```typescript
const db = new DB({
  sqlitePath: './data.sqlite',
  s3Config: { bucket: 'my-bucket', region: 'us-east-1' },
  autoSync: false,
});

// Do work...
await Users.create({ name: 'Alice', email: 'alice@example.com' });

// Flush when ready
await db.flush();
```

### Monitoring sync metrics

```typescript
const metrics = db.getMetrics();

console.log({
  isRunning:     metrics.isRunning,       // Is sync engine active
  pendingChanges: metrics.pendingChanges, // Un-synced log entries
  totalSynced:   metrics.totalSynced,     // Total entries synced this session
  syncErrors:    metrics.syncErrors,      // Cumulative error count
  dbSizeBytes:   metrics.dbSizeBytes,     // SQLite file size
  lastSyncAt:    metrics.lastSyncAt,      // Timestamp of last successful sync
  lastSnapshotAt: metrics.lastSnapshotAt, // Timestamp of last snapshot
});
```

### Raw SQL access

```typescript
const db = new DB({ sqlitePath: './data.sqlite' });

// Via repository
const Users = db.define('users', schema);
const rows = Users.raw<{ name: string; cnt: number }>(
  'SELECT name, COUNT(*) as cnt FROM users GROUP BY name HAVING cnt > ?',
  [5]
);

// Via raw better-sqlite3 handle
const sqlite = db.raw();
sqlite.prepare('CREATE INDEX IF NOT EXISTS ...').run();
```

### Multiple models with relations

```typescript
const Teams = db.define('teams', {
  name: Column.text().required().build(),
});

const Members = db.define('members', {
  teamId: Column.text().required().index().build(),
  userId: Column.text().required().build(),
  role:   Column.text().default('member').build(),
});

const team  = await Teams.create({ name: 'Engineering' });
const alice = await Users.create({ name: 'Alice', email: 'a@example.com' });
await Members.create({ teamId: team.id, userId: alice.id, role: 'lead' });

// Query with raw SQL for joins
const teamMembers = Members.raw<{ name: string; role: string }>(
  `SELECT u.name, m.role
   FROM members m
   JOIN users u ON u.id = m.userId
   WHERE m.teamId = ?`,
  [team.id]
);
```

---

## Trade-offs & Design Decisions

### Why SQLite?

SQLite is a battle-tested, zero-dependency embedded database. It runs in-process with no separate server, handles GB-scale datasets, and supports full ACID transactions. `better-sqlite3` gives us a synchronous API that maps naturally to local-first patterns.

### Why S3 for persistence?

S3 is the simplest, cheapest, and most universally available durable object store. There's no infrastructure to manage, it scales infinitely, and 11 nines of durability is effectively permanent. For backup and replication purposes, it's a perfect fit.

### Why incremental logs instead of full DB sync?

Uploading the full SQLite file on every write would be:
- Slow for large databases (GB-scale)
- Expensive in S3 PUT costs
- Bandwidth-intensive

Incremental log batching means only the delta is uploaded. A 100-entry batch might be a few KB compressed, regardless of total DB size.

### Why not CRDTs or multi-master sync?

DriftDB is designed for **single-writer per node** scenarios. If you need multi-master conflict resolution, CRDTs (like those in `cr-sqlite`) are the right tool. DriftDB prioritizes simplicity and reliability for the common case.

### Why is sync one-way by default?

Pushing local changes to S3 is the core use case. Pull-based restore (downloading the latest snapshot) is supported via `SnapshotManager.restoreLatest()` but is intentionally a manual operation — DriftDB does not auto-merge remote changes into your local DB during runtime.

### SQLite WAL mode trade-off

WAL mode (`journal_mode = WAL`) means:
- ✅ Readers don't block writers
- ✅ Writers don't block readers
- ✅ Crash safety — incomplete writes are rolled back
- ⚠️ Two extra files: `.sqlite-wal` and `.sqlite-shm` (normal, safe to ignore)
- ⚠️ Slightly larger disk footprint until `wal_checkpoint` runs

DriftDB automatically runs `wal_checkpoint(FULL)` before every snapshot to keep WAL size bounded.

---

## License

MIT
