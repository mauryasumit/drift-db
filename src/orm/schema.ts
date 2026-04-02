import type { ColumnDef, ColumnType, ModelSchema } from '../types.js';

export class ColumnBuilder {
  private readonly def: ColumnDef;

  constructor(type: ColumnType) {
    this.def = { type };
  }

  required(): this {
    this.def.notNull = true;
    return this;
  }

  unique(): this {
    this.def.unique = true;
    return this;
  }

  default(value: string | number | boolean | null): this {
    this.def.default = value;
    return this;
  }

  index(): this {
    this.def.index = true;
    return this;
  }

  build(): ColumnDef {
    return { ...this.def };
  }
}

export const Column = {
  text(): ColumnBuilder {
    return new ColumnBuilder('TEXT');
  },
  integer(): ColumnBuilder {
    return new ColumnBuilder('INTEGER');
  },
  real(): ColumnBuilder {
    return new ColumnBuilder('REAL');
  },
  blob(): ColumnBuilder {
    return new ColumnBuilder('BLOB');
  },
  boolean(): ColumnBuilder {
    return new ColumnBuilder('BOOLEAN');
  },
};

export function buildCreateTableSQL(tableName: string, schema: ModelSchema): string {
  const cols: string[] = [
    'id TEXT PRIMARY KEY',
    'createdAt INTEGER NOT NULL',
    'updatedAt INTEGER NOT NULL',
  ];

  const indices: string[] = [];

  for (const [name, rawDef] of Object.entries(schema)) {
    const def: ColumnDef =
      rawDef instanceof ColumnBuilder ? rawDef.build() : rawDef;

    let col = `"${name}" ${def.type}`;
    if (def.notNull) col += ' NOT NULL';
    if (def.unique) col += ' UNIQUE';
    if (def.default !== undefined) {
      const v = def.default;
      col += ` DEFAULT ${typeof v === 'string' ? `'${v}'` : String(v)}`;
    }
    cols.push(col);

    if (def.index) {
      indices.push(
        `CREATE INDEX IF NOT EXISTS "idx_${tableName}_${name}" ON "${tableName}" ("${name}");`
      );
    }
  }

  const create = `CREATE TABLE IF NOT EXISTS "${tableName}" (\n  ${cols.join(',\n  ')}\n);`;
  return [create, ...indices].join('\n');
}

export function normalizeSchema(schema: ModelSchema): ModelSchema {
  const normalized: ModelSchema = {};
  for (const [key, val] of Object.entries(schema)) {
    normalized[key] = val instanceof ColumnBuilder ? val.build() : val;
  }
  return normalized;
}
