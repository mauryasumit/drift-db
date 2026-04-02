import { DB, Model, Column } from '../src/index';
import type { ModelSchema, WhereClause } from '../src/index';

describe('Repository - define API', () => {
  let db: DB;

  beforeEach(() => {
    db = new DB({ sqlitePath: ':memory:', autoSync: false });
  });

  afterEach(() => {
    db.close();
  });

  const userSchema: ModelSchema = {
    name: { type: 'TEXT', notNull: true },
    email: { type: 'TEXT', unique: true },
    age: { type: 'INTEGER' },
  };

  test('create and findById', async () => {
    const Users = db.define('users', userSchema);
    const user = await Users.create({ name: 'Alice', email: 'alice@test.com', age: 30 });

    expect(user.id).toBeDefined();
    expect(user.name).toBe('Alice');
    expect(user.email).toBe('alice@test.com');
    expect(user.createdAt).toBeGreaterThan(0);

    const found = await Users.findById(user.id);
    expect(found).not.toBeNull();
    expect(found!.id).toBe(user.id);
  });

  test('filter with equality', async () => {
    const Users = db.define('users2', userSchema);
    await Users.create({ name: 'Alice', email: 'a@test.com' });
    await Users.create({ name: 'Bob', email: 'b@test.com' });

    const results = await Users.filter({ name: 'Alice' } as WhereClause<typeof results[0]>);
    expect(results).toHaveLength(1);
    expect(results[0]?.name).toBe('Alice');
  });

  test('filter with operators', async () => {
    const Users = db.define('users3', userSchema);
    await Users.create({ name: 'Alice', email: 'a3@test.com', age: 25 });
    await Users.create({ name: 'Bob', email: 'b3@test.com', age: 35 });
    await Users.create({ name: 'Carol', email: 'c3@test.com', age: 20 });

    const adults = await Users.filter({ age: { $gte: 25 } } as WhereClause<typeof adults[0]>);
    expect(adults).toHaveLength(2);

    const young = await Users.filter({ age: { $lt: 25 } } as WhereClause<typeof young[0]>);
    expect(young).toHaveLength(1);
    expect(young[0]?.name).toBe('Carol');
  });

  test('filter with $in', async () => {
    const Users = db.define('users4', userSchema);
    await Users.create({ name: 'Alice', email: 'a4@test.com' });
    await Users.create({ name: 'Bob', email: 'b4@test.com' });
    await Users.create({ name: 'Carol', email: 'c4@test.com' });

    const results = await Users.filter({ name: { $in: ['Alice', 'Carol'] } } as WhereClause<typeof results[0]>);
    expect(results).toHaveLength(2);
  });

  test('update', async () => {
    const Users = db.define('users5', userSchema);
    const user = await Users.create({ name: 'Alice', email: 'a5@test.com' });

    const updated = await Users.update({ id: user.id } as WhereClause<typeof user>, { name: 'Alice Smith' });
    expect(updated).toBe(1);

    const found = await Users.findById(user.id);
    expect(found!.name).toBe('Alice Smith');
    expect(found!.updatedAt).toBeGreaterThanOrEqual(user.updatedAt);
  });

  test('delete', async () => {
    const Users = db.define('users6', userSchema);
    const user = await Users.create({ name: 'Alice', email: 'a6@test.com' });

    const deleted = await Users.delete({ id: user.id } as WhereClause<typeof user>);
    expect(deleted).toBe(1);

    const found = await Users.findById(user.id);
    expect(found).toBeNull();
  });

  test('count', async () => {
    const Users = db.define('users7', userSchema);
    await Users.create({ name: 'Alice', email: 'a7@test.com' });
    await Users.create({ name: 'Bob', email: 'b7@test.com' });

    const total = await Users.count();
    expect(total).toBe(2);
  });

  test('upsert - creates when not found', async () => {
    const Users = db.define('users8', userSchema);
    const user = await Users.upsert({ email: 'new@test.com' } as WhereClause<typeof user>, { name: 'New', email: 'new@test.com' });
    expect(user.name).toBe('New');
  });

  test('upsert - updates when found', async () => {
    const Users = db.define('users9', userSchema);
    const created = await Users.create({ name: 'Original', email: 'upd@test.com' });
    const upserted = await Users.upsert({ id: created.id } as WhereClause<typeof upserted>, { name: 'Updated' });
    expect(upserted.name).toBe('Updated');
    expect(upserted.id).toBe(created.id);
  });

  test('find with orderBy and limit', async () => {
    const Users = db.define('users10', userSchema);
    await Users.create({ name: 'Charlie', email: 'charlie@test.com', age: 30 });
    await Users.create({ name: 'Alice', email: 'alice2@test.com', age: 20 });
    await Users.create({ name: 'Bob', email: 'bob2@test.com', age: 25 });

    const sorted = await Users.find({
      orderBy: { age: 'ASC' } as Partial<Record<string, 'ASC' | 'DESC'>>,
      limit: 2,
    });
    expect(sorted).toHaveLength(2);
    expect(sorted[0]?.age).toBe(20);
    expect(sorted[1]?.age).toBe(25);
  });

  test('Column builder generates correct schema', async () => {
    const schema: ModelSchema = {
      title: Column.text().required().build(),
      score: Column.real().default(0).build(),
      active: Column.boolean().default(true).build(),
    };
    const Posts = db.define('posts', schema);
    const post = await Posts.create({ title: 'Hello', score: 9.5, active: 1 });
    expect(post.title).toBe('Hello');
  });
});

describe('Model - class-based API', () => {
  let db: DB;

  beforeEach(() => {
    db = new DB({ sqlitePath: ':memory:', autoSync: false });
  });

  afterEach(() => {
    db.close();
  });

  test('registerModel and CRUD', async () => {
    class Product extends Model {
      static tableName = 'products';
      static schema: ModelSchema = {
        name: { type: 'TEXT', notNull: true },
        price: { type: 'REAL' },
      };

      name!: string;
      price!: number;
    }

    db.registerModel(Product);

    const p = await Product.create({ name: 'Widget', price: 9.99 }) as Product;
    expect(p.id).toBeDefined();
    expect(p.name).toBe('Widget');

    const found = await Product.findById(p.id) as Product | null;
    expect(found!.price).toBe(9.99);

    const all = await Product.filter();
    expect(all).toHaveLength(1);

    await Product.update({ id: p.id }, { price: 19.99 } as Partial<Omit<Product, 'id' | 'createdAt'>>);
    const updated = await Product.findById(p.id) as Product | null;
    expect(updated!.price).toBe(19.99);

    await Product.delete({ id: p.id });
    const deleted = await Product.findById(p.id);
    expect(deleted).toBeNull();
  });

  test('throws if tableName missing', () => {
    class NoName extends Model {
      static tableName = '';
      static schema: ModelSchema = {};
    }
    expect(() => db.registerModel(NoName)).toThrow();
  });
});

describe('DB - utility methods', () => {
  test('integrityCheck returns true for healthy DB', () => {
    const db = new DB({ sqlitePath: ':memory:', autoSync: false });
    expect(db.integrityCheck()).toBe(true);
    db.close();
  });

  test('transaction rolls back on error', async () => {
    const db = new DB({ sqlitePath: ':memory:', autoSync: false });
    const Items = db.define('items', { name: { type: 'TEXT' } });
    await Items.create({ name: 'before' });

    try {
      db.transaction(() => {
        db.raw().prepare(`INSERT INTO items (id, createdAt, updatedAt, name) VALUES ('x', 1, 1, 'in-tx')`).run();
        throw new Error('rollback!');
      });
    } catch { /* expected */ }

    const count = await Items.count();
    expect(count).toBe(1);
    db.close();
  });
});
