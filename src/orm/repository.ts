import type Database from 'better-sqlite3';
import type { BaseRecord, FindOptions, ModelSchema, WhereClause } from '../types.js';
import { buildCreateTableSQL, normalizeSchema } from './schema.js';
import { buildSelectSQL, buildWhereClause } from './query-builder.js';
import { generateId } from '../utils/id.js';
import type { ChangeLog } from '../sync/change-log.js';

export type RecordOf<S extends ModelSchema> = BaseRecord & {
  [K in keyof S]: S[K] extends { type: 'INTEGER' }
    ? number
    : S[K] extends { type: 'REAL' }
    ? number
    : S[K] extends { type: 'BOOLEAN' }
    ? boolean
    : S[K] extends { type: 'BLOB' }
    ? Buffer
    : string;
};

export class Repository<T extends BaseRecord> {
  private readonly db: Database.Database;
  private readonly tableName: string;
  private readonly schema: ModelSchema;
  private readonly changeLog: ChangeLog | null;

  constructor(
    db: Database.Database,
    tableName: string,
    schema: ModelSchema,
    changeLog: ChangeLog | null
  ) {
    this.db = db;
    this.tableName = tableName;
    this.schema = normalizeSchema(schema);
    this.changeLog = changeLog;
    this.initTable();
  }

  private initTable(): void {
    const sql = buildCreateTableSQL(this.tableName, this.schema);
    this.db.exec(sql);
  }

  async create(data: Partial<Omit<T, 'id' | 'createdAt' | 'updatedAt'>>): Promise<T> {
    const now = Date.now();
    const id = generateId();
    const record = { id, createdAt: now, updatedAt: now, ...data } as T;

    const keys = Object.keys(record);
    const placeholders = keys.map(() => '?').join(', ');
    const cols = keys.map((k) => `"${k}"`).join(', ');
    const values = keys.map((k) => (record as Record<string, unknown>)[k]);

    this.db
      .prepare(`INSERT INTO "${this.tableName}" (${cols}) VALUES (${placeholders})`)
      .run(...values);

    this.changeLog?.append(this.tableName, 'insert', record as Record<string, unknown>);

    return record;
  }

  async findById(id: string): Promise<T | null> {
    const row = this.db
      .prepare(`SELECT * FROM "${this.tableName}" WHERE id = ?`)
      .get(id) as T | undefined;
    return row ?? null;
  }

  async findOne(where: WhereClause<T>): Promise<T | null> {
    const { sql, params } = buildSelectSQL<T>(this.tableName, { where, limit: 1 });
    const row = this.db.prepare(sql).get(...params) as T | undefined;
    return row ?? null;
  }

  async find(options: FindOptions<T> = {}): Promise<T[]> {
    const { sql, params } = buildSelectSQL<T>(this.tableName, options);
    return this.db.prepare(sql).all(...params) as T[];
  }

  async filter(where: WhereClause<T> = {}, options: Omit<FindOptions<T>, 'where'> = {}): Promise<T[]> {
    return this.find({ where, ...options });
  }

  async update(where: WhereClause<T>, data: Partial<Omit<T, 'id' | 'createdAt'>>): Promise<number> {
    const now = Date.now();
    const updateData = { ...data, updatedAt: now };

    const setCols = Object.keys(updateData)
      .map((k) => `"${k}" = ?`)
      .join(', ');
    const setValues = Object.values(updateData);

    const { sql: whereSQL, params: whereParams } = buildWhereClause(where);
    const sql = `UPDATE "${this.tableName}" SET ${setCols} ${whereSQL}`;

    const result = this.db.prepare(sql).run(...setValues, ...whereParams);

    if (result.changes > 0) {
      this.changeLog?.append(this.tableName, 'update', {
        where: where as Record<string, unknown>,
        data: updateData as Record<string, unknown>,
      });
    }

    return result.changes;
  }

  async delete(where: WhereClause<T>): Promise<number> {
    const { sql: whereSQL, params: whereParams } = buildWhereClause(where);
    const sql = `DELETE FROM "${this.tableName}" ${whereSQL}`;

    const result = this.db.prepare(sql).run(...whereParams);

    if (result.changes > 0) {
      this.changeLog?.append(this.tableName, 'delete', {
        where: where as Record<string, unknown>,
      });
    }

    return result.changes;
  }

  async deleteById(id: string): Promise<boolean> {
    const changes = await this.delete({ id } as WhereClause<T>);
    return changes > 0;
  }

  async count(where: WhereClause<T> = {}): Promise<number> {
    const { sql: whereSQL, params } = buildWhereClause(where);
    const sql = `SELECT COUNT(*) as cnt FROM "${this.tableName}" ${whereSQL}`;
    const row = this.db.prepare(sql).get(...params) as { cnt: number };
    return row.cnt;
  }

  async upsert(
    where: WhereClause<T>,
    data: Partial<Omit<T, 'id' | 'createdAt'>>
  ): Promise<T> {
    const existing = await this.findOne(where);
    if (existing) {
      await this.update(where, data);
      return (await this.findById(existing.id))!;
    }
    return this.create(data as Partial<Omit<T, 'id' | 'createdAt' | 'updatedAt'>>);
  }

  raw<R = unknown>(sql: string, params: unknown[] = []): R[] {
    return this.db.prepare(sql).all(...params) as R[];
  }

  runRaw(sql: string, params: unknown[] = []): { changes: number; lastInsertRowid: number | bigint } {
    const result = this.db.prepare(sql).run(...params);
    return { changes: result.changes, lastInsertRowid: result.lastInsertRowid };
  }
}
