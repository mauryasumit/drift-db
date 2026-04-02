import { DB, Model, Column } from '../src/index';
import type { ModelSchema } from '../src/index';

async function schemaBasedExample() {
  const db = await DB.open({
    dbName: 'myapp',
    sqlitePath: './data/myapp.sqlite',
    autoRestore: true,
    s3Config: {
      bucket: 'my-app-data',
      region: 'us-east-1',
      prefix: 'driftdb',
    },
    logger: (event) => {
      console.log(`[${event.scope}] ${event.message}`, event.metadata ?? '');
    },
    syncIntervalMs: 5_000,
    snapshotEveryNLogs: 500,
    compression: true,
    retryConfig: {
      maxRetries: 5,
      baseDelayMs: 500,
      maxDelayMs: 30_000,
    },
  });

  const Users = db.define('users', {
    name: Column.text().required().build(),
    email: Column.text().unique().required().build(),
    age: Column.integer().build(),
    active: Column.boolean().default(true).build(),
  });

  const alice = await Users.create({ name: 'Alice', email: 'alice@example.com', age: 30 });
  console.log('Created:', alice);

  const users = await Users.filter({ active: 1 });
  console.log('Active users:', users.length);

  const adults = await Users.find({
    where: { age: { $gte: 18 } },
    orderBy: { name: 'ASC' },
    limit: 10,
  });
  console.log('Adults:', adults.length);

  await Users.update({ id: alice.id }, { name: 'Alice Smith' });

  const found = await Users.findById(alice.id);
  console.log('Updated:', found?.name);

  const metrics = db.getMetrics();
  console.log('Pending changes:', metrics.pendingChanges);
  console.log('Total synced:', metrics.totalSynced);
  console.log('DB size (bytes):', metrics.dbSizeBytes);

  await db.flush();
  console.log('Flush complete');

  db.close();
}

async function classBasedExample() {
  const db = new DB({
    dbName: 'posts-demo',
    sqlitePath: ':memory:',
    autoSync: false,
  });

  class Post extends Model {
    static tableName = 'posts';
    static schema: ModelSchema = {
      title: Column.text().required().build(),
      content: Column.text().build(),
      published: Column.boolean().default(false).build(),
      views: Column.integer().default(0).build(),
    };

    title!: string;
    content!: string;
    published!: boolean;
    views!: number;
  }

  db.registerModel(Post);

  const post = await Post.create({
    title: 'Hello DriftDB',
    content: 'Local-first databases are the future.',
    published: false,
  });
  console.log('Post created:', post.id);

  await Post.update({ id: post.id }, { published: true });

  const published = await Post.filter({ published: 1 as unknown as boolean });
  console.log('Published posts:', published.length);

  const count = await Post.count();
  console.log('Total posts:', count);

  const upserted = await Post.upsert(
    { title: 'Hello DriftDB' },
    { title: 'Hello DriftDB', views: 100 }
  );
  console.log('Upserted post views:', upserted.views);

  await Post.delete({ id: post.id });
  console.log('Post deleted');

  console.log('Integrity check:', db.integrityCheck());

  db.close();
}

async function failureRecoveryExample() {
  const db = new DB({
    dbName: 'recovery-demo',
    sqlitePath: ':memory:',
    s3Config: {
      bucket: 'recovery-bucket',
      region: 'us-east-1',
    },
    retryConfig: {
      maxRetries: 3,
      baseDelayMs: 200,
      maxDelayMs: 5_000,
    },
    autoSync: false,
  });

  const Events = db.define('events', {
    type: Column.text().required().build(),
    payload: Column.text().build(),
    processedAt: Column.integer().build(),
  });

  db.transaction(() => {
    for (let i = 0; i < 100; i++) {
      Events.create({ type: 'pageview', payload: `{"page":"/page-${i}"}` }).catch(console.error);
    }
  });

  const pendingMetrics = db.getMetrics();
  console.log('Pending changes after batch insert:', pendingMetrics.pendingChanges);

  db.close();
}

void schemaBasedExample().catch(console.error);
void classBasedExample().catch(console.error);
void failureRecoveryExample().catch(console.error);
