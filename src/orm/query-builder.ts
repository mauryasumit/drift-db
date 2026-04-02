import type { WhereClause, FindOptions } from '../types.js';

export interface QueryParts {
  sql: string;
  params: unknown[];
}

export function buildWhereClause<T>(where: WhereClause<T>): QueryParts {
  const conditions: string[] = [];
  const params: unknown[] = [];

  for (const [key, value] of Object.entries(where)) {
    if (value === undefined) continue;

    if (value !== null && typeof value === 'object' && !Array.isArray(value)) {
      const ops = value as Record<string, unknown>;

      if ('$gt' in ops) {
        conditions.push(`"${key}" > ?`);
        params.push(ops['$gt']);
      }
      if ('$gte' in ops) {
        conditions.push(`"${key}" >= ?`);
        params.push(ops['$gte']);
      }
      if ('$lt' in ops) {
        conditions.push(`"${key}" < ?`);
        params.push(ops['$lt']);
      }
      if ('$lte' in ops) {
        conditions.push(`"${key}" <= ?`);
        params.push(ops['$lte']);
      }
      if ('$ne' in ops) {
        conditions.push(`"${key}" != ?`);
        params.push(ops['$ne']);
      }
      if ('$like' in ops) {
        conditions.push(`"${key}" LIKE ?`);
        params.push(ops['$like']);
      }
      if ('$in' in ops && Array.isArray(ops['$in'])) {
        const arr = ops['$in'] as unknown[];
        if (arr.length === 0) {
          conditions.push('0 = 1');
        } else {
          conditions.push(`"${key}" IN (${arr.map(() => '?').join(',')})`);
          params.push(...arr);
        }
      }
    } else {
      if (value === null) {
        conditions.push(`"${key}" IS NULL`);
      } else {
        conditions.push(`"${key}" = ?`);
        params.push(value);
      }
    }
  }

  return {
    sql: conditions.length > 0 ? `WHERE ${conditions.join(' AND ')}` : '',
    params,
  };
}

export function buildSelectSQL<T>(
  tableName: string,
  options: FindOptions<T> = {}
): QueryParts {
  const parts: string[] = [`SELECT * FROM "${tableName}"`];
  const params: unknown[] = [];

  if (options.where && Object.keys(options.where).length > 0) {
    const { sql, params: wParams } = buildWhereClause(options.where);
    if (sql) {
      parts.push(sql);
      params.push(...wParams);
    }
  }

  if (options.orderBy) {
    const orderClauses = Object.entries(options.orderBy)
      .map(([col, dir]) => `"${col}" ${dir ?? 'ASC'}`)
      .join(', ');
    if (orderClauses) parts.push(`ORDER BY ${orderClauses}`);
  }

  if (options.limit !== undefined) {
    parts.push(`LIMIT ?`);
    params.push(options.limit);
  }

  if (options.offset !== undefined) {
    parts.push(`OFFSET ?`);
    params.push(options.offset);
  }

  return { sql: parts.join(' '), params };
}
