import type { SQL } from 'drizzle-orm';
import { integer, pgTable, text } from 'drizzle-orm/pg-core';
import { describe, expect, it, vi } from 'vitest';
import type { DataSchema, PaginationQueryParams } from 'zod-paginate';
import {
  applyDrizzlePaginationOnQuery,
  createMySqlDrizzleOperators,
  createPgDrizzleOperators,
} from './drizzle-adapter';

const users = pgTable('users', {
  id: integer('id').notNull(),
  name: text('name'),
  age: integer('age'),
  tags: text('tags').array(),
});

class QuerySpy {
  public readonly whereCalls: SQL[] = [];
  public readonly orderByCalls: SQL[][] = [];
  public readonly limitCalls: number[] = [];
  public readonly offsetCalls: number[] = [];

  public then<TResult1 = Record<string, unknown>[], TResult2 = never>(
    onfulfilled?: ((value: Record<string, unknown>[]) => TResult1 | PromiseLike<TResult1>) | null,
    onrejected?: ((reason: unknown) => TResult2 | PromiseLike<TResult2>) | null,
  ): PromiseLike<TResult1 | TResult2> {
    return Promise.resolve<Record<string, unknown>[]>([]).then(onfulfilled, onrejected);
  }

  public $dynamic(): this {
    return this;
  }

  public where(expression: SQL): this {
    this.whereCalls.push(expression);
    return this;
  }

  public orderBy(...expressions: SQL[]): this {
    this.orderByCalls.push(expressions);
    return this;
  }

  public limit(value: number): this {
    this.limitCalls.push(value);
    return this;
  }

  public offset(value: number): this {
    this.offsetCalls.push(value);
    return this;
  }
}

function toParsed(
  pagination: PaginationQueryParams<DataSchema>['pagination'],
): PaginationQueryParams<DataSchema> {
  return { pagination };
}

describe('createPgDrizzleOperators', () => {
  it('exposes contains support for postgres', () => {
    const operators = createPgDrizzleOperators();
    expect(operators.contains).toBeTypeOf('function');
  });
});

describe('createMySqlDrizzleOperators', () => {
  it('does not expose contains support by default', () => {
    const operators = createMySqlDrizzleOperators();
    expect(operators.contains).toBeUndefined();
  });
});

describe('applyDrizzlePaginationOnQuery', () => {
  it('applies where, orderBy, limit and offset for limit/offset pagination', () => {
    const query = new QuerySpy();

    const parsed = toParsed({
      type: 'LIMIT_OFFSET',
      page: 2,
      limit: 10,
      sortBy: [{ property: 'name', direction: 'DESC' }],
      filters: {
        type: 'filter',
        field: 'name',
        condition: { group: 'name', op: '$eq', value: 'alice' },
      },
    });

    const result = applyDrizzlePaginationOnQuery(parsed, {
      dialect: 'pg',
      buildQuery: () => query,
      fields: {
        name: users.name,
      },
    });

    expect(result.query).toBe(query);
    expect(result.clauses.limit).toBe(10);
    expect(result.clauses.offset).toBe(10);
    expect(query.whereCalls).toHaveLength(1);
    expect(query.orderByCalls).toHaveLength(1);
    expect(query.orderByCalls[0]).toHaveLength(1);
    expect(query.limitCalls).toEqual([10]);
    expect(query.offsetCalls).toEqual([10]);
  });

  it('builds a select shape and returns it in clauses without applying it on the query', () => {
    const query = new QuerySpy();

    const parsed = toParsed({
      type: 'LIMIT_OFFSET',
      page: 1,
      limit: 5,
      select: ['profile.name', 'profile_name'],
    });

    const result = applyDrizzlePaginationOnQuery(parsed, {
      dialect: 'pg',
      buildQuery: () => query,
      fields: {
        'profile.name': users.name,
        profile_name: users.age,
      },
    });

    expect(result.clauses.select).toBeDefined();
    expect(result.clauses.select).toHaveProperty('profile_name');
    expect(result.clauses.select).toHaveProperty('profile_name_1');
  });

  it('passes the select shape to the buildQuery callback', () => {
    const query = new QuerySpy();
    const buildQuery = vi.fn(() => query);

    const parsed = toParsed({
      type: 'LIMIT_OFFSET',
      page: 1,
      limit: 5,
      select: ['name', 'age'],
    });

    applyDrizzlePaginationOnQuery(parsed, {
      dialect: 'pg',
      buildQuery,
      fields: {
        name: users.name,
        age: users.age,
      },
    });

    expect(buildQuery).toHaveBeenCalledTimes(1);
    expect(buildQuery).toHaveBeenCalledWith(
      expect.objectContaining({
        name: users.name,
        age: users.age,
      }),
    );
  });

  it('ignores unknown mapped fields when strictFieldMapping is disabled', () => {
    const query = new QuerySpy();
    const parsed = toParsed({
      type: 'LIMIT_OFFSET',
      page: 1,
      limit: 10,
      sortBy: [{ property: 'unknownSort', direction: 'ASC' }],
      select: ['unknownSelect'],
      filters: {
        type: 'filter',
        field: 'unknownFilter',
        condition: { group: 'unknownFilter', op: '$eq', value: 'x' },
      },
    });

    const result = applyDrizzlePaginationOnQuery(parsed, {
      dialect: 'pg',
      buildQuery: () => query,
      fields: {
        name: users.name,
      },
      strictFieldMapping: false,
    });

    expect(result.clauses.select).toEqual({});
    expect(result.clauses.where).toBeUndefined();
    expect(result.clauses.orderBy).toBeUndefined();
    expect(query.whereCalls).toHaveLength(0);
    expect(query.orderByCalls).toHaveLength(0);
  });

  it('throws when a field is missing and strictFieldMapping is enabled', () => {
    const query = new QuerySpy();
    const parsed = toParsed({
      type: 'LIMIT_OFFSET',
      page: 1,
      limit: 10,
      filters: {
        type: 'filter',
        field: 'missing',
        condition: { group: 'missing', op: '$eq', value: 1 },
      },
    });

    expect(() =>
      applyDrizzlePaginationOnQuery(parsed, {
        dialect: 'pg',
        buildQuery: () => query,
        fields: {
          name: users.name,
        },
        strictFieldMapping: true,
      }),
    ).toThrow('No Drizzle field mapping found for "missing"');
  });

  it('supports all scalar comparison operators and not modifier', () => {
    const query = new QuerySpy();
    const parsed = toParsed({
      type: 'LIMIT_OFFSET',
      page: 1,
      limit: 20,
      filters: {
        type: 'and',
        items: [
          {
            type: 'filter',
            field: 'age',
            condition: { group: 'age', op: '$gt', value: 18 },
          },
          {
            type: 'filter',
            field: 'age',
            condition: { group: 'age', op: '$gte', value: 21 },
          },
          {
            type: 'filter',
            field: 'age',
            condition: { group: 'age', op: '$lt', value: 65 },
          },
          {
            type: 'filter',
            field: 'age',
            condition: { group: 'age', op: '$lte', value: 64, not: true },
          },
          {
            type: 'filter',
            field: 'age',
            condition: { group: 'age', op: '$btw', value: [30, 40] },
          },
        ],
      },
    });

    const result = applyDrizzlePaginationOnQuery(parsed, {
      dialect: 'pg',
      buildQuery: () => query,
      fields: {
        age: users.age,
      },
    });

    expect(result.clauses.where).toBeDefined();
    expect(query.whereCalls).toHaveLength(1);
  });

  it('supports ilike and starts-with operators', () => {
    const query = new QuerySpy();
    const parsed = toParsed({
      type: 'LIMIT_OFFSET',
      page: 1,
      limit: 10,
      filters: {
        type: 'or',
        items: [
          {
            type: 'filter',
            field: 'name',
            condition: { group: 'name', op: '$ilike', value: 'al_ice%' },
          },
          {
            type: 'filter',
            field: 'name',
            condition: { group: 'name', op: '$sw', value: 'jo%n' },
          },
        ],
      },
    });

    const result = applyDrizzlePaginationOnQuery(parsed, {
      dialect: 'pg',
      buildQuery: () => query,
      fields: {
        name: users.name,
      },
    });

    expect(result.clauses.where).toBeDefined();
    expect(query.whereCalls).toHaveLength(1);
  });

  it('supports contains with postgres operators', () => {
    const query = new QuerySpy();
    const parsed = toParsed({
      type: 'LIMIT_OFFSET',
      page: 1,
      limit: 10,
      filters: {
        type: 'filter',
        field: 'tags',
        condition: { group: 'tags', op: '$contains', value: ['premium'] },
      },
    });

    const result = applyDrizzlePaginationOnQuery(parsed, {
      dialect: 'pg',
      buildQuery: () => query,
      fields: {
        tags: users.tags,
      },
    });

    expect(result.clauses.where).toBeDefined();
    expect(query.whereCalls).toHaveLength(1);
  });

  it('throws on contains when operator set has no contains implementation', () => {
    const query = new QuerySpy();
    const parsed = toParsed({
      type: 'LIMIT_OFFSET',
      page: 1,
      limit: 10,
      filters: {
        type: 'filter',
        field: 'name',
        condition: { group: 'name', op: '$contains', value: ['x'] },
      },
    });

    expect(() =>
      applyDrizzlePaginationOnQuery(parsed, {
        dialect: 'mysql',
        buildQuery: () => query,
        fields: {
          name: users.name,
        },
      }),
    ).toThrow(
      'Operator "$contains" is present but no "contains" function is provided in the Drizzle operator set',
    );
  });

  it('returns undefined where for composite filters without mapped children in non strict mode', () => {
    const query = new QuerySpy();
    const parsed = toParsed({
      type: 'LIMIT_OFFSET',
      page: 1,
      limit: 10,
      filters: {
        type: 'and',
        items: [
          {
            type: 'filter',
            field: 'unknown',
            condition: { group: 'unknown', op: '$eq', value: 'x' },
          },
        ],
      },
    });

    const result = applyDrizzlePaginationOnQuery(parsed, {
      dialect: 'pg',
      buildQuery: () => query,
      fields: {
        name: users.name,
      },
      strictFieldMapping: false,
    });

    expect(result.clauses.where).toBeUndefined();
    expect(query.whereCalls).toHaveLength(0);
  });

  it('keeps single mapped child from composite filters in non strict mode', () => {
    const query = new QuerySpy();
    const parsed = toParsed({
      type: 'LIMIT_OFFSET',
      page: 1,
      limit: 10,
      filters: {
        type: 'and',
        items: [
          {
            type: 'filter',
            field: 'unknown',
            condition: { group: 'unknown', op: '$eq', value: 'x' },
          },
          {
            type: 'filter',
            field: 'name',
            condition: { group: 'name', op: '$eq', value: 'alice' },
          },
        ],
      },
    });

    const result = applyDrizzlePaginationOnQuery(parsed, {
      dialect: 'pg',
      buildQuery: () => query,
      fields: {
        name: users.name,
      },
      strictFieldMapping: false,
    });

    expect(result.clauses.where).toBeDefined();
    expect(query.whereCalls).toHaveLength(1);
  });

  it('normalizes offset when page is zero', () => {
    const query = new QuerySpy();
    const parsed = toParsed({
      type: 'LIMIT_OFFSET',
      page: 0,
      limit: 10,
    });

    const result = applyDrizzlePaginationOnQuery(parsed, {
      dialect: 'pg',
      buildQuery: () => query,
      fields: {
        name: users.name,
      },
    });

    expect(result.clauses.offset).toBe(0);
    expect(query.offsetCalls).toEqual([0]);
  });

  it('supports $null filter operator', () => {
    const query = new QuerySpy();
    const parsed = toParsed({
      type: 'LIMIT_OFFSET',
      page: 1,
      limit: 10,
      filters: {
        type: 'filter',
        field: 'name',
        condition: { group: 'name', op: '$null' },
      },
    });

    const result = applyDrizzlePaginationOnQuery(parsed, {
      dialect: 'pg',
      buildQuery: () => query,
      fields: { name: users.name },
    });

    expect(result.clauses.where).toBeDefined();
    expect(query.whereCalls).toHaveLength(1);
  });

  it('supports $in filter operator', () => {
    const query = new QuerySpy();
    const parsed = toParsed({
      type: 'LIMIT_OFFSET',
      page: 1,
      limit: 10,
      filters: {
        type: 'filter',
        field: 'name',
        condition: { group: 'name', op: '$in', value: ['alice', 'bob'] },
      },
    });

    const result = applyDrizzlePaginationOnQuery(parsed, {
      dialect: 'pg',
      buildQuery: () => query,
      fields: { name: users.name },
    });

    expect(result.clauses.where).toBeDefined();
    expect(query.whereCalls).toHaveLength(1);
  });

  it('applies clauses using mysql dialect without errors', () => {
    const query = new QuerySpy();
    const parsed = toParsed({
      type: 'LIMIT_OFFSET',
      page: 1,
      limit: 10,
      sortBy: [{ property: 'name', direction: 'ASC' }],
      filters: {
        type: 'filter',
        field: 'name',
        condition: { group: 'name', op: '$ilike', value: 'alice' },
      },
    });

    const result = applyDrizzlePaginationOnQuery(parsed, {
      dialect: 'mysql',
      buildQuery: () => query,
      fields: { name: users.name },
    });

    expect(result.clauses.where).toBeDefined();
    expect(result.clauses.orderBy).toHaveLength(1);
    expect(query.whereCalls).toHaveLength(1);
    expect(query.orderByCalls).toHaveLength(1);
  });

  it('returns empty select shape when parsed has no select', () => {
    const query = new QuerySpy();
    const parsed = toParsed({
      type: 'LIMIT_OFFSET',
      page: 1,
      limit: 10,
    });

    const result = applyDrizzlePaginationOnQuery(parsed, {
      dialect: 'pg',
      buildQuery: () => query,
      fields: { name: users.name },
    });

    expect(result.clauses.select).toEqual({});
  });

  it('applies custom selectAlias when building the select shape', () => {
    const query = new QuerySpy();
    const parsed = toParsed({
      type: 'LIMIT_OFFSET',
      page: 1,
      limit: 5,
      select: ['name'],
    });

    const result = applyDrizzlePaginationOnQuery(parsed, {
      dialect: 'pg',
      buildQuery: () => query,
      fields: { name: users.name },
      selectAlias: (path) => `col_${path}`,
    });

    expect(result.clauses.select).toHaveProperty('col_name', users.name);
  });

  it('accepts custom operators override', () => {
    const query = new QuerySpy();
    const operators = createPgDrizzleOperators();
    const parsed = toParsed({
      type: 'LIMIT_OFFSET',
      page: 1,
      limit: 10,
      filters: {
        type: 'filter',
        field: 'name',
        condition: { group: 'name', op: '$eq', value: 'test' },
      },
    });

    const result = applyDrizzlePaginationOnQuery(parsed, {
      dialect: 'pg',
      buildQuery: () => query,
      fields: { name: users.name },
      operators,
    });

    expect(result.clauses.where).toBeDefined();
    expect(query.whereCalls).toHaveLength(1);
  });
});
