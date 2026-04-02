import type { BaseRecord, FindOptions, ModelSchema, WhereClause } from '../types.js';
import type { Repository } from './repository.js';

export abstract class Model implements BaseRecord {
  id!: string;
  createdAt!: number;
  updatedAt!: number;

  static tableName: string;
  static schema: ModelSchema;

  static _repo: Repository<BaseRecord>;

  static async create<T extends Model>(
    this: ModelStatic<T>,
    data: Partial<Omit<T, 'id' | 'createdAt' | 'updatedAt'>>
  ): Promise<T> {
    return (this._repo as Repository<T>).create(data) as Promise<T>;
  }

  static async findById<T extends Model>(
    this: ModelStatic<T>,
    id: string
  ): Promise<T | null> {
    return (this._repo as Repository<T>).findById(id) as Promise<T | null>;
  }

  static async findOne<T extends Model>(
    this: ModelStatic<T>,
    where: WhereClause<T>
  ): Promise<T | null> {
    return (this._repo as Repository<T>).findOne(where) as Promise<T | null>;
  }

  static async find<T extends Model>(
    this: ModelStatic<T>,
    options: FindOptions<T> = {}
  ): Promise<T[]> {
    return (this._repo as Repository<T>).find(options) as Promise<T[]>;
  }

  static async filter<T extends Model>(
    this: ModelStatic<T>,
    where: WhereClause<T> = {},
    options: Omit<FindOptions<T>, 'where'> = {}
  ): Promise<T[]> {
    return (this._repo as Repository<T>).filter(where, options) as Promise<T[]>;
  }

  static async update<T extends Model>(
    this: ModelStatic<T>,
    where: WhereClause<T>,
    data: Partial<Omit<T, 'id' | 'createdAt'>>
  ): Promise<number> {
    return (this._repo as Repository<T>).update(where, data);
  }

  static async delete<T extends Model>(
    this: ModelStatic<T>,
    where: WhereClause<T>
  ): Promise<number> {
    return (this._repo as Repository<T>).delete(where);
  }

  static async count<T extends Model>(
    this: ModelStatic<T>,
    where: WhereClause<T> = {}
  ): Promise<number> {
    return (this._repo as Repository<T>).count(where);
  }

  static async upsert<T extends Model>(
    this: ModelStatic<T>,
    where: WhereClause<T>,
    data: Partial<Omit<T, 'id' | 'createdAt'>>
  ): Promise<T> {
    return (this._repo as Repository<T>).upsert(where, data) as Promise<T>;
  }
}

export interface ModelStatic<T extends Model = Model> {
  new(): T;
  tableName: string;
  schema: ModelSchema;
  _repo: Repository<BaseRecord>;
  create(data: Partial<Omit<T, 'id' | 'createdAt' | 'updatedAt'>>): Promise<T>;
  findById(id: string): Promise<T | null>;
  findOne(where: WhereClause<T>): Promise<T | null>;
  find(options?: FindOptions<T>): Promise<T[]>;
  filter(where?: WhereClause<T>, options?: Omit<FindOptions<T>, 'where'>): Promise<T[]>;
  update(where: WhereClause<T>, data: Partial<Omit<T, 'id' | 'createdAt'>>): Promise<number>;
  delete(where: WhereClause<T>): Promise<number>;
  count(where?: WhereClause<T>): Promise<number>;
  upsert(where: WhereClause<T>, data: Partial<Omit<T, 'id' | 'createdAt'>>): Promise<T>;
}
