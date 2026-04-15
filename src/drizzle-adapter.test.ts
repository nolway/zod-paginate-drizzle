import { sql, type SQL } from 'drizzle-orm';
import { integer, pgTable, text } from 'drizzle-orm/pg-core';
import { describe, expect, it, vi } from 'vitest';
import { z } from 'zod';
import type { AllowedSelectablePath, PaginationPayload, SelectQueryPayload } from 'zod-paginate';
import {
  applyDrizzlePaginationOnQuery,
  generatePaginationQuery,
  generateSelectQuery,
  assembleDrizzleRelations,
  buildCursorResponseMeta,
  buildLimitOffsetResponseMeta,
  createMySqlDrizzleOperators,
  createPgDrizzleOperators,
} from './drizzle-adapter';

/**
 * Concrete Zod schema covering all field paths used across tests.
 * This avoids the abstract `DataSchema` type where `AllowedSelectablePath`
 * collapses to `never`.
 */
const TestSchema = z.object({
  id: z.number(),
  name: z.string(),
  email: z.string(),
  age: z.number(),
  tags: z.array(z.string()),
  profile_name: z.string(),
  profile: z.object({ name: z.string() }),
  posts: z.object({ id: z.number(), title: z.string() }),
  comments: z.object({ body: z.string() }),
});
type TestSchema = typeof TestSchema;

const users = pgTable('users', {
  id: integer('id').notNull(),
  name: text('name'),
  age: integer('age'),
  tags: text('tags').array(),
});

const postsTable = pgTable('posts', {
  id: integer('id').notNull(),
  title: text('title'),
  authorId: integer('author_id'),
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

function toParsed(pagination: PaginationPayload<TestSchema>): PaginationPayload<TestSchema> {
  return pagination;
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
      // @ts-expect-error -- intentionally invalid field names for unknown mapping test
      sortBy: [{ property: 'unknownSort', direction: 'ASC' }],
      // @ts-expect-error -- intentionally invalid field names for unknown mapping test
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

  it('applies cursor-based WHERE clause for CURSOR pagination (ASC)', () => {
    const query = new QuerySpy();

    const parsed = toParsed({
      type: 'CURSOR',
      limit: 10,
      cursor: 42,
      cursorProperty: 'id',
      sortBy: [{ property: 'id', direction: 'ASC' }],
    });

    const result = applyDrizzlePaginationOnQuery(parsed, {
      dialect: 'pg',
      buildQuery: (): QuerySpy => query,
      fields: { id: users.id, name: users.name },
    });

    expect(result.clauses.cursor).toBe(42);
    expect(result.clauses.cursorProperty).toBe('id');
    expect(query.whereCalls).toHaveLength(1);
    // No offset should be applied for cursor pagination.
    expect(query.offsetCalls).toHaveLength(0);
    expect(query.limitCalls).toHaveLength(1);
    expect(query.limitCalls[0]).toBe(10);
  });

  it('applies cursor-based WHERE clause for CURSOR pagination (DESC)', () => {
    const query = new QuerySpy();

    const parsed = toParsed({
      type: 'CURSOR',
      limit: 5,
      cursor: 100,
      cursorProperty: 'id',
      sortBy: [{ property: 'id', direction: 'DESC' }],
    });

    const result = applyDrizzlePaginationOnQuery(parsed, {
      dialect: 'pg',
      buildQuery: (): QuerySpy => query,
      fields: { id: users.id, name: users.name },
    });

    expect(result.clauses.cursor).toBe(100);
    expect(query.whereCalls).toHaveLength(1);
  });

  it('applies no cursor WHERE when cursor is undefined', () => {
    const query = new QuerySpy();

    const parsed = toParsed({
      type: 'CURSOR',
      limit: 10,
      cursorProperty: 'id',
    });

    const result = applyDrizzlePaginationOnQuery(parsed, {
      dialect: 'pg',
      buildQuery: (): QuerySpy => query,
      fields: { id: users.id },
    });

    expect(result.clauses.cursor).toBeUndefined();
    expect(result.clauses.cursorProperty).toBe('id');
    expect(query.whereCalls).toHaveLength(0);
  });

  it('combines cursor WHERE with existing filters', () => {
    const query = new QuerySpy();

    const parsed = toParsed({
      type: 'CURSOR',
      limit: 10,
      cursor: 5,
      cursorProperty: 'id',
      sortBy: [{ property: 'id', direction: 'ASC' }],
      filters: {
        type: 'filter',
        field: 'name',
        condition: { group: 'name', op: '$eq', value: 'alice' },
      },
    });

    const result = applyDrizzlePaginationOnQuery(parsed, {
      dialect: 'pg',
      buildQuery: (): QuerySpy => query,
      fields: { id: users.id, name: users.name },
    });

    expect(result.clauses.cursor).toBe(5);
    // Both the filter and cursor condition should be combined into where.
    expect(query.whereCalls).toHaveLength(1);
  });
});

describe('generatePaginationQuery', () => {
  it('builds a main query and separate relation queries', () => {
    const mainQuery = new QuerySpy();
    const relationQuery = new QuerySpy();

    const parsed = toParsed({
      type: 'LIMIT_OFFSET',
      page: 1,
      limit: 10,
      select: ['name', 'posts.title'],
      sortBy: [{ property: 'name', direction: 'ASC' }],
    });

    const result = generatePaginationQuery(parsed, {
      dialect: 'pg',
      buildQuery: (): QuerySpy => mainQuery,
      fields: { id: users.id, name: users.name },
      relations: [
        {
          relationName: 'posts',
          fields: { id: postsTable.id, title: postsTable.title },
          foreignKey: postsTable.authorId,
          parentKey: users.id,
          buildQuery: (): QuerySpy => relationQuery,
        },
      ],
    });

    expect(result.query).toBe(mainQuery);
    expect(result.relationQueries).toHaveLength(1);
    expect(result.relationQueries[0]?.relationName).toBe('posts');
    expect(result.relationQueries[0]?.foreignKeyAlias).toBe('__fk');
  });

  it('strips relation-prefixed fields from the main select', () => {
    const mainQuery = new QuerySpy();
    const relationQuery = new QuerySpy();
    const buildMainQuery = vi.fn((): QuerySpy => mainQuery);

    const parsed = toParsed({
      type: 'LIMIT_OFFSET',
      page: 1,
      limit: 10,
      select: ['name', 'posts.title', 'posts.id'],
    });

    generatePaginationQuery(parsed, {
      dialect: 'pg',
      buildQuery: buildMainQuery,
      fields: { id: users.id, name: users.name },
      relations: [
        {
          relationName: 'posts',
          fields: { id: postsTable.id, title: postsTable.title },
          foreignKey: postsTable.authorId,
          parentKey: users.id,
          buildQuery: (): QuerySpy => relationQuery,
        },
      ],
    });

    // Main query should have 'name' + '__pk_posts' (parent key), but not 'posts.title'
    expect(buildMainQuery).toHaveBeenCalledTimes(1);
    expect(buildMainQuery).toHaveBeenCalledWith(
      expect.objectContaining({ name: users.name, __pk_posts: users.id }),
    );
    expect(buildMainQuery).not.toHaveBeenCalledWith(
      expect.objectContaining({ posts_title: expect.anything() }),
    );
  });

  it('routes relation-prefixed filters to the relation query only', () => {
    const mainQuery = new QuerySpy();
    const relationQuery = new QuerySpy();

    const parsed = toParsed({
      type: 'LIMIT_OFFSET',
      page: 1,
      limit: 10,
      filters: {
        type: 'and',
        items: [
          {
            type: 'filter',
            field: 'name',
            condition: { group: 'name', op: '$eq', value: 'alice' },
          },
          {
            type: 'filter',
            field: 'posts.title',
            condition: { group: 'posts.title', op: '$ilike', value: 'hello' },
          },
        ],
      },
    });

    const result = generatePaginationQuery(parsed, {
      dialect: 'pg',
      buildQuery: (): QuerySpy => mainQuery,
      fields: { name: users.name },
      relations: [
        {
          relationName: 'posts',
          fields: { title: postsTable.title },
          foreignKey: postsTable.authorId,
          parentKey: users.id,
          buildQuery: (): QuerySpy => relationQuery,
        },
      ],
    });

    // Main query has only the name filter
    expect(mainQuery.whereCalls).toHaveLength(1);
    // Relation query has only the title filter
    expect(relationQuery.whereCalls).toHaveLength(1);
    expect(result.relationQueries).toHaveLength(1);
  });

  it('routes relation-prefixed sortBy to the relation query only', () => {
    const mainQuery = new QuerySpy();
    const relationQuery = new QuerySpy();

    const parsed = toParsed({
      type: 'LIMIT_OFFSET',
      page: 1,
      limit: 10,
      sortBy: [
        { property: 'name', direction: 'ASC' },
        { property: 'posts.title', direction: 'DESC' },
      ],
    });

    generatePaginationQuery(parsed, {
      dialect: 'pg',
      buildQuery: (): QuerySpy => mainQuery,
      fields: { name: users.name },
      relations: [
        {
          relationName: 'posts',
          fields: { title: postsTable.title },
          foreignKey: postsTable.authorId,
          parentKey: users.id,
          buildQuery: (): QuerySpy => relationQuery,
        },
      ],
    });

    // Main query sorted by name only
    expect(mainQuery.orderByCalls).toHaveLength(1);
    expect(mainQuery.orderByCalls[0]).toHaveLength(1);
    // Relation query sorted by title only
    expect(relationQuery.orderByCalls).toHaveLength(1);
    expect(relationQuery.orderByCalls[0]).toHaveLength(1);
  });

  it('applies static relation orderBy on the relation query', () => {
    const relationQuery = new QuerySpy();
    const staticOrder = [sql`1`];

    const parsed = toParsed({
      type: 'LIMIT_OFFSET',
      page: 1,
      limit: 10,
    });

    generatePaginationQuery(parsed, {
      dialect: 'pg',
      buildQuery: (): QuerySpy => new QuerySpy(),
      fields: { name: users.name },
      relations: [
        {
          relationName: 'posts',
          fields: { title: postsTable.title },
          foreignKey: postsTable.authorId,
          parentKey: users.id,
          orderBy: staticOrder,
          buildQuery: (): QuerySpy => relationQuery,
        },
      ],
    });

    expect(relationQuery.orderByCalls).toHaveLength(1);
    expect(relationQuery.orderByCalls[0]).toEqual(staticOrder);
  });

  it('appends static relation orderBy after client-requested sort', () => {
    const relationQuery = new QuerySpy();
    const staticOrder = [sql`2`];

    const parsed = toParsed({
      type: 'LIMIT_OFFSET',
      page: 1,
      limit: 10,
      sortBy: [{ property: 'posts.title', direction: 'ASC' }],
    });

    generatePaginationQuery(parsed, {
      dialect: 'pg',
      buildQuery: (): QuerySpy => new QuerySpy(),
      fields: { name: users.name },
      relations: [
        {
          relationName: 'posts',
          fields: { title: postsTable.title },
          foreignKey: postsTable.authorId,
          parentKey: users.id,
          orderBy: staticOrder,
          buildQuery: (): QuerySpy => relationQuery,
        },
      ],
    });

    // One orderBy call with client sort + static sort concatenated
    expect(relationQuery.orderByCalls).toHaveLength(1);
    expect(relationQuery.orderByCalls[0]).toHaveLength(2);
    // Static orderBy is at the end (tiebreaker)
    expect(relationQuery.orderByCalls[0]?.[1]).toBe(staticOrder[0]);
  });

  it('passes relation limit through to relationQueries', () => {
    const parsed = toParsed({
      type: 'LIMIT_OFFSET',
      page: 1,
      limit: 10,
    });

    const result = generatePaginationQuery(parsed, {
      dialect: 'pg',
      buildQuery: (): QuerySpy => new QuerySpy(),
      fields: { name: users.name },
      relations: [
        {
          relationName: 'posts',
          fields: { title: postsTable.title },
          foreignKey: postsTable.authorId,
          parentKey: users.id,
          limit: 3,
          buildQuery: (): QuerySpy => new QuerySpy(),
        },
      ],
    });

    expect(result.relationQueries[0]?.limit).toBe(3);
  });

  it('does not apply limit/offset on relation queries', () => {
    const mainQuery = new QuerySpy();
    const relationQuery = new QuerySpy();

    const parsed = toParsed({
      type: 'LIMIT_OFFSET',
      page: 2,
      limit: 5,
    });

    generatePaginationQuery(parsed, {
      dialect: 'pg',
      buildQuery: (): QuerySpy => mainQuery,
      fields: { name: users.name },
      relations: [
        {
          relationName: 'posts',
          fields: { title: postsTable.title },
          foreignKey: postsTable.authorId,
          parentKey: users.id,
          buildQuery: (): QuerySpy => relationQuery,
        },
      ],
    });

    // Main query has limit + offset
    expect(mainQuery.limitCalls).toEqual([5]);
    expect(mainQuery.offsetCalls).toEqual([5]);
    // Relation query has neither
    expect(relationQuery.limitCalls).toHaveLength(0);
    expect(relationQuery.offsetCalls).toHaveLength(0);
  });

  it('always injects parent key into main select shape', () => {
    const mainQuery = new QuerySpy();
    const relationQuery = new QuerySpy();
    const buildMainQuery = vi.fn((): QuerySpy => mainQuery);

    const parsed = toParsed({
      type: 'LIMIT_OFFSET',
      page: 1,
      limit: 10,
    });

    generatePaginationQuery(parsed, {
      dialect: 'pg',
      buildQuery: buildMainQuery,
      fields: { name: users.name },
      relations: [
        {
          relationName: 'posts',
          fields: { title: postsTable.title },
          foreignKey: postsTable.authorId,
          parentKey: users.id,
          buildQuery: (): QuerySpy => relationQuery,
        },
      ],
    });

    expect(buildMainQuery).toHaveBeenCalledWith(expect.objectContaining({ __pk_posts: users.id }));
  });

  it('always injects foreign key into relation select shape', () => {
    const mainQuery = new QuerySpy();
    const buildRelationQuery = vi.fn((): QuerySpy => new QuerySpy());

    const parsed = toParsed({
      type: 'LIMIT_OFFSET',
      page: 1,
      limit: 10,
      select: ['posts.title'],
    });

    generatePaginationQuery(parsed, {
      dialect: 'pg',
      buildQuery: (): QuerySpy => mainQuery,
      fields: { name: users.name },
      relations: [
        {
          relationName: 'posts',
          fields: { title: postsTable.title },
          foreignKey: postsTable.authorId,
          parentKey: users.id,
          buildQuery: buildRelationQuery,
        },
      ],
    });

    expect(buildRelationQuery).toHaveBeenCalledTimes(1);
    expect(buildRelationQuery).toHaveBeenCalledWith(
      expect.objectContaining({
        __fk: postsTable.authorId,
        title: postsTable.title,
      }),
    );
  });

  it('exposes assemble() that assembles main rows with relation results', () => {
    const parsed = toParsed({
      type: 'LIMIT_OFFSET',
      page: 1,
      limit: 10,
      select: ['name', 'posts.title'],
    });

    const result = generatePaginationQuery(parsed, {
      dialect: 'pg',
      buildQuery: (): QuerySpy => new QuerySpy(),
      fields: { name: users.name },
      relations: [
        {
          relationName: 'posts',
          fields: { title: postsTable.title },
          foreignKey: postsTable.authorId,
          parentKey: users.id,
          buildQuery: (): QuerySpy => new QuerySpy(),
        },
      ],
    });

    const mainRows = [{ __pk_posts: 1, name: 'Alice' }];
    const relationResults = [[{ __fk: 1, title: 'Hello' }]];

    const assembled = result.assemble(mainRows, relationResults);

    expect(assembled).toHaveLength(1);
    expect(assembled[0]).toEqual({ name: 'Alice', posts: [{ title: 'Hello' }] });
  });

  it('execute() returns { data, pagination } for LIMIT_OFFSET', async () => {
    const mainSpy = new QuerySpy();
    const relationSpy = new QuerySpy();
    const countSpy = new QuerySpy();

    // Make QuerySpy resolve with specific data.
    const mainData = [{ __pk_posts: 1, name: 'Alice' }];
    const relationData = [{ __fk: 1, title: 'Hello' }];
    const countData = [{ count: 42 }];

    vi.spyOn(mainSpy, 'then').mockImplementation((onfulfilled) =>
      Promise.resolve(mainData).then(onfulfilled),
    );
    vi.spyOn(relationSpy, 'then').mockImplementation((onfulfilled) =>
      Promise.resolve(relationData).then(onfulfilled),
    );
    vi.spyOn(countSpy, 'then').mockImplementation((onfulfilled) =>
      Promise.resolve(countData).then(onfulfilled),
    );

    const parsed = toParsed({
      type: 'LIMIT_OFFSET',
      page: 2,
      limit: 10,
      select: ['name', 'posts.title'],
    });

    // buildQuery is called twice: once for main query, once for count query.
    const spies = [mainSpy, countSpy];
    let callIdx = 0;

    const result = generatePaginationQuery(parsed, {
      dialect: 'pg',
      buildQuery: (): QuerySpy => {
        const spy = spies[callIdx++];
        if (!spy) throw new Error('Unexpected buildQuery call');
        return spy;
      },
      fields: { name: users.name },
      relations: [
        {
          relationName: 'posts',
          fields: { title: postsTable.title },
          foreignKey: postsTable.authorId,
          parentKey: users.id,
          buildQuery: (): QuerySpy => relationSpy,
        },
      ],
    });

    const { data, pagination } = await result.execute();

    expect(data).toHaveLength(1);
    expect(data[0]).toEqual({ name: 'Alice', posts: [{ title: 'Hello' }] });
    expect(pagination).toEqual(
      expect.objectContaining({
        itemsPerPage: 10,
        totalItems: 42,
        currentPage: 2,
        totalPages: 5,
      }),
    );
  });

  it('execute() returns { data, pagination } for CURSOR', async () => {
    const mainSpy = new QuerySpy();
    const mainData = [
      { __pk_posts: 1, id: 43, name: 'Alice' },
      { __pk_posts: 2, id: 44, name: 'Bob' },
    ];

    vi.spyOn(mainSpy, 'then').mockImplementation((onfulfilled) =>
      Promise.resolve(mainData).then(onfulfilled),
    );

    const parsed = toParsed({
      type: 'CURSOR',
      limit: 10,
      cursor: 42,
      cursorProperty: 'id',
      select: ['id', 'name', 'posts.title'],
      sortBy: [{ property: 'id', direction: 'ASC' }],
    });

    const result = generatePaginationQuery(parsed, {
      dialect: 'pg',
      buildQuery: (): QuerySpy => mainSpy,
      fields: { id: users.id, name: users.name },
      relations: [
        {
          relationName: 'posts',
          fields: { title: postsTable.title },
          foreignKey: postsTable.authorId,
          parentKey: users.id,
          buildQuery: (): QuerySpy => new QuerySpy(),
        },
      ],
    });

    const { data, pagination } = await result.execute();

    expect(data).toHaveLength(2);
    expect(pagination).toEqual(
      expect.objectContaining({
        itemsPerPage: 10,
        cursor: 44,
      }),
    );
  });

  it('injects cursorProperty into select shape when not explicitly selected', () => {
    const buildMainQuery = vi.fn((): QuerySpy => new QuerySpy());

    const parsed = toParsed({
      type: 'CURSOR',
      limit: 10,
      cursorProperty: 'id',
      select: ['name'],
      sortBy: [{ property: 'id', direction: 'ASC' }],
    });

    const result = generatePaginationQuery(parsed, {
      dialect: 'pg',
      buildQuery: buildMainQuery,
      fields: { id: users.id, name: users.name },
    });

    expect(buildMainQuery).toHaveBeenCalledWith(expect.objectContaining({ id: users.id }));
    expect(result.clauses.cursorProperty).toBe('id');
  });

  it('does not duplicate cursorProperty when already selected', () => {
    const buildMainQuery = vi.fn((): QuerySpy => new QuerySpy());

    const parsed = toParsed({
      type: 'CURSOR',
      limit: 10,
      cursorProperty: 'id',
      select: ['id', 'name'],
      sortBy: [{ property: 'id', direction: 'ASC' }],
    });

    generatePaginationQuery(parsed, {
      dialect: 'pg',
      buildQuery: buildMainQuery,
      fields: { id: users.id, name: users.name },
    });

    expect(buildMainQuery).toHaveBeenCalledTimes(1);
    expect(buildMainQuery).toHaveBeenCalledWith(expect.objectContaining({ id: users.id }));
  });
});

describe('generateSelectQuery', () => {
  function toSelectParsed(
    select: AllowedSelectablePath<TestSchema>[],
  ): SelectQueryPayload<TestSchema> {
    return { fields: select, responseType: 'many' };
  }

  it('partitions select paths between main query and relations', () => {
    const mainQuery = new QuerySpy();
    const buildRelationQuery = vi.fn((): QuerySpy => new QuerySpy());

    const parsed = toSelectParsed(['name', 'posts.title']);

    const result = generateSelectQuery(parsed, {
      buildQuery: (): QuerySpy => mainQuery,
      fields: { name: users.name },
      relations: [
        {
          relationName: 'posts',
          fields: { title: postsTable.title },
          foreignKey: postsTable.authorId,
          parentKey: users.id,
          buildQuery: buildRelationQuery,
        },
      ],
    });

    expect(result.query).toBe(mainQuery);
    expect(result.relationQueries).toHaveLength(1);
    expect(result.relationQueries[0]?.relationName).toBe('posts');
  });

  it('injects parent key aliases into the main select shape', () => {
    const buildMainQuery = vi.fn((): QuerySpy => new QuerySpy());

    const parsed = toSelectParsed(['name']);

    generateSelectQuery(parsed, {
      buildQuery: buildMainQuery,
      fields: { name: users.name },
      relations: [
        {
          relationName: 'posts',
          fields: { title: postsTable.title },
          foreignKey: postsTable.authorId,
          parentKey: users.id,
          buildQuery: (): QuerySpy => new QuerySpy(),
        },
      ],
    });

    expect(buildMainQuery).toHaveBeenCalledTimes(1);
    expect(buildMainQuery).toHaveBeenCalledWith(
      expect.objectContaining({
        __pk_posts: users.id,
        name: users.name,
      }),
    );
  });

  it('always injects foreign key into relation select shape', () => {
    const buildRelationQuery = vi.fn((): QuerySpy => new QuerySpy());

    const parsed = toSelectParsed(['posts.title']);

    generateSelectQuery(parsed, {
      buildQuery: (): QuerySpy => new QuerySpy(),
      fields: { name: users.name },
      relations: [
        {
          relationName: 'posts',
          fields: { title: postsTable.title },
          foreignKey: postsTable.authorId,
          parentKey: users.id,
          buildQuery: buildRelationQuery,
        },
      ],
    });

    expect(buildRelationQuery).toHaveBeenCalledTimes(1);
    expect(buildRelationQuery).toHaveBeenCalledWith(
      expect.objectContaining({
        __fk: postsTable.authorId,
        title: postsTable.title,
      }),
    );
  });

  it('does not apply where, orderBy, limit or offset on the main query', () => {
    const mainQuery = new QuerySpy();

    const parsed = toSelectParsed(['name', 'posts.title']);

    generateSelectQuery(parsed, {
      buildQuery: (): QuerySpy => mainQuery,
      fields: { name: users.name },
      relations: [
        {
          relationName: 'posts',
          fields: { title: postsTable.title },
          foreignKey: postsTable.authorId,
          parentKey: users.id,
          buildQuery: (): QuerySpy => new QuerySpy(),
        },
      ],
    });

    expect(mainQuery.whereCalls).toHaveLength(0);
    expect(mainQuery.orderByCalls).toHaveLength(0);
    expect(mainQuery.limitCalls).toHaveLength(0);
    expect(mainQuery.offsetCalls).toHaveLength(0);
  });

  it('does not apply where or orderBy on relation queries', () => {
    const relationQuery = new QuerySpy();

    const parsed = toSelectParsed(['posts.title']);

    generateSelectQuery(parsed, {
      buildQuery: (): QuerySpy => new QuerySpy(),
      fields: { name: users.name },
      relations: [
        {
          relationName: 'posts',
          fields: { title: postsTable.title },
          foreignKey: postsTable.authorId,
          parentKey: users.id,
          buildQuery: (): QuerySpy => relationQuery,
        },
      ],
    });

    expect(relationQuery.whereCalls).toHaveLength(0);
    expect(relationQuery.orderByCalls).toHaveLength(0);
  });

  it('applies static relation orderBy on the relation query', () => {
    const relationQuery = new QuerySpy();
    const staticOrder = [sql`1`];

    const parsed = toSelectParsed(['posts.title']);

    generateSelectQuery(parsed, {
      buildQuery: (): QuerySpy => new QuerySpy(),
      fields: { name: users.name },
      relations: [
        {
          relationName: 'posts',
          fields: { title: postsTable.title },
          foreignKey: postsTable.authorId,
          parentKey: users.id,
          orderBy: staticOrder,
          buildQuery: (): QuerySpy => relationQuery,
        },
      ],
    });

    expect(relationQuery.orderByCalls).toHaveLength(1);
    expect(relationQuery.orderByCalls[0]).toEqual(staticOrder);
  });

  it('passes relation limit through to relationQueries', () => {
    const parsed = toSelectParsed(['posts.title']);

    const result = generateSelectQuery(parsed, {
      buildQuery: (): QuerySpy => new QuerySpy(),
      fields: { name: users.name },
      relations: [
        {
          relationName: 'posts',
          fields: { title: postsTable.title },
          foreignKey: postsTable.authorId,
          parentKey: users.id,
          limit: 5,
          buildQuery: (): QuerySpy => new QuerySpy(),
        },
      ],
    });

    expect(result.relationQueries[0]?.limit).toBe(5);
  });

  it('works with assembleDrizzleRelations for end-to-end flow', () => {
    const parsed = toSelectParsed(['name', 'posts.title']);

    const result = generateSelectQuery(parsed, {
      buildQuery: (): QuerySpy => new QuerySpy(),
      fields: { name: users.name },
      relations: [
        {
          relationName: 'posts',
          fields: { title: postsTable.title },
          foreignKey: postsTable.authorId,
          parentKey: users.id,
          buildQuery: (): QuerySpy => new QuerySpy(),
        },
      ],
    });

    const mainRows = [
      { __pk_posts: 1, name: 'Alice' },
      { __pk_posts: 2, name: 'Bob' },
    ];

    const relationResults = [
      [
        { __fk: 1, title: 'Post A1' },
        { __fk: 2, title: 'Post B1' },
      ],
    ];

    const assembled = assembleDrizzleRelations(mainRows, result.relationQueries, relationResults);

    expect(assembled).toHaveLength(2);
    expect(assembled[0]).toEqual({ name: 'Alice', posts: [{ title: 'Post A1' }] });
    expect(assembled[1]).toEqual({ name: 'Bob', posts: [{ title: 'Post B1' }] });
  });

  it('handles multiple relations', () => {
    const parsed = toSelectParsed(['name', 'posts.title', 'comments.body']);

    const commentsTable = pgTable('comments', {
      id: integer('id').notNull(),
      body: text('body'),
      userId: integer('user_id'),
    });

    const result = generateSelectQuery(parsed, {
      buildQuery: (): QuerySpy => new QuerySpy(),
      fields: { name: users.name },
      relations: [
        {
          relationName: 'posts',
          fields: { title: postsTable.title },
          foreignKey: postsTable.authorId,
          parentKey: users.id,
          buildQuery: (): QuerySpy => new QuerySpy(),
        },
        {
          relationName: 'comments',
          fields: { body: commentsTable.body },
          foreignKey: commentsTable.userId,
          parentKey: users.id,
          buildQuery: (): QuerySpy => new QuerySpy(),
        },
      ],
    });

    expect(result.relationQueries).toHaveLength(2);
    expect(result.relationQueries[0]?.relationName).toBe('posts');
    expect(result.relationQueries[1]?.relationName).toBe('comments');
  });

  it('exposes assemble() that assembles main rows with relation results', () => {
    const parsed = toSelectParsed(['name', 'posts.title']);

    const result = generateSelectQuery(parsed, {
      buildQuery: (): QuerySpy => new QuerySpy(),
      fields: { name: users.name },
      relations: [
        {
          relationName: 'posts',
          fields: { title: postsTable.title },
          foreignKey: postsTable.authorId,
          parentKey: users.id,
          buildQuery: (): QuerySpy => new QuerySpy(),
        },
      ],
    });

    const mainRows = [{ __pk_posts: 1, name: 'Alice' }];
    const relationResults = [[{ __fk: 1, title: 'Hello' }]];

    const assembled = result.assemble(mainRows, relationResults);

    expect(assembled).toHaveLength(1);
    expect(assembled[0]).toEqual({ name: 'Alice', posts: [{ title: 'Hello' }] });
  });

  it('execute() returns { data } with assembled relations', async () => {
    const mainSpy = new QuerySpy();
    const relationSpy = new QuerySpy();

    const mainData = [{ __pk_posts: 1, name: 'Alice' }];
    const relationData = [{ __fk: 1, title: 'Hello' }];

    vi.spyOn(mainSpy, 'then').mockImplementation((onfulfilled) =>
      Promise.resolve(mainData).then(onfulfilled),
    );
    vi.spyOn(relationSpy, 'then').mockImplementation((onfulfilled) =>
      Promise.resolve(relationData).then(onfulfilled),
    );

    const parsed = toSelectParsed(['name', 'posts.title']);

    const result = generateSelectQuery(parsed, {
      buildQuery: (): QuerySpy => mainSpy,
      fields: { name: users.name },
      relations: [
        {
          relationName: 'posts',
          fields: { title: postsTable.title },
          foreignKey: postsTable.authorId,
          parentKey: users.id,
          buildQuery: (): QuerySpy => relationSpy,
        },
      ],
    });

    const { data } = await result.execute();

    expect(data).toHaveLength(1);
    expect(data[0]).toEqual({ name: 'Alice', posts: [{ title: 'Hello' }] });
  });

  it('applies limit 1 on main query when responseType is "one"', () => {
    const mainQuery = new QuerySpy();

    const parsed: SelectQueryPayload<TestSchema> = {
      fields: ['name'],
      responseType: 'one',
    };

    generateSelectQuery(parsed, {
      buildQuery: (): QuerySpy => mainQuery,
      fields: { name: users.name },
    });

    expect(mainQuery.limitCalls).toEqual([1]);
  });

  it('does not apply limit on main query when responseType is "many"', () => {
    const mainQuery = new QuerySpy();

    const parsed: SelectQueryPayload<TestSchema> = {
      fields: ['name'],
      responseType: 'many',
    };

    generateSelectQuery(parsed, {
      buildQuery: (): QuerySpy => mainQuery,
      fields: { name: users.name },
    });

    expect(mainQuery.limitCalls).toHaveLength(0);
  });

  it('does not apply limit on main query when responseType is undefined', () => {
    const mainQuery = new QuerySpy();

    const parsed = toSelectParsed(['name']);

    generateSelectQuery(parsed, {
      buildQuery: (): QuerySpy => mainQuery,
      fields: { name: users.name },
    });

    expect(mainQuery.limitCalls).toHaveLength(0);
  });

  it('execute() returns a single object when responseType is "one"', async () => {
    const mainSpy = new QuerySpy();

    const mainData = [{ name: 'Alice' }];

    vi.spyOn(mainSpy, 'then').mockImplementation((onfulfilled) =>
      Promise.resolve(mainData).then(onfulfilled),
    );

    const parsed: SelectQueryPayload<TestSchema> = {
      fields: ['name'],
      responseType: 'one',
    };

    const result = generateSelectQuery(parsed, {
      buildQuery: (): QuerySpy => mainSpy,
      fields: { name: users.name },
    });

    const { data } = await result.execute();

    expect(data).toEqual({ name: 'Alice' });
  });

  it('execute() returns null when responseType is "one" and no rows', async () => {
    const mainSpy = new QuerySpy();

    vi.spyOn(mainSpy, 'then').mockImplementation((onfulfilled) =>
      Promise.resolve([]).then(onfulfilled),
    );

    const parsed: SelectQueryPayload<TestSchema> = {
      fields: ['name'],
      responseType: 'one',
    };

    const result = generateSelectQuery(parsed, {
      buildQuery: (): QuerySpy => mainSpy,
      fields: { name: users.name },
    });

    const executeResult = await result.execute();

    expect(executeResult).toBeNull();
  });

  it('execute() returns an array when responseType is "many"', async () => {
    const mainSpy = new QuerySpy();

    const mainData = [{ name: 'Alice' }, { name: 'Bob' }];

    vi.spyOn(mainSpy, 'then').mockImplementation((onfulfilled) =>
      Promise.resolve(mainData).then(onfulfilled),
    );

    const parsed: SelectQueryPayload<TestSchema> = {
      fields: ['name'],
      responseType: 'many',
    };

    const result = generateSelectQuery(parsed, {
      buildQuery: (): QuerySpy => mainSpy,
      fields: { name: users.name },
    });

    const { data } = await result.execute();

    expect(data).toEqual([{ name: 'Alice' }, { name: 'Bob' }]);
  });

  it('execute() returns single object with relations when responseType is "one"', async () => {
    const mainSpy = new QuerySpy();
    const relationSpy = new QuerySpy();

    const mainData = [{ __pk_posts: 1, name: 'Alice' }];
    const relationData = [{ __fk: 1, title: 'Hello' }];

    vi.spyOn(mainSpy, 'then').mockImplementation((onfulfilled) =>
      Promise.resolve(mainData).then(onfulfilled),
    );
    vi.spyOn(relationSpy, 'then').mockImplementation((onfulfilled) =>
      Promise.resolve(relationData).then(onfulfilled),
    );

    const parsed: SelectQueryPayload<TestSchema> = {
      fields: ['name', 'posts.title'],
      responseType: 'one',
    };

    const result = generateSelectQuery(parsed, {
      buildQuery: (): QuerySpy => mainSpy,
      fields: { name: users.name },
      relations: [
        {
          relationName: 'posts',
          fields: { title: postsTable.title },
          foreignKey: postsTable.authorId,
          parentKey: users.id,
          buildQuery: (): QuerySpy => relationSpy,
        },
      ],
    });

    const { data } = await result.execute();

    expect(data).toEqual({ name: 'Alice', posts: [{ title: 'Hello' }] });
  });
});

describe('assembleDrizzleRelations', () => {
  it('groups child rows under their parent by foreign key', () => {
    const mainRows = [
      { __pk_posts: 1, name: 'Alice' },
      { __pk_posts: 2, name: 'Bob' },
    ];

    const relationQueries = [
      {
        relationName: 'posts',
        parentKey: users.id,
        foreignKeyAlias: '__fk',
        mode: 'many' as const,
        query: new QuerySpy(),
      },
    ];

    const relationResults = [
      [
        { __fk: 1, title: 'Post A1' },
        { __fk: 1, title: 'Post A2' },
        { __fk: 2, title: 'Post B1' },
      ],
    ];

    const assembled = assembleDrizzleRelations(mainRows, relationQueries, relationResults);

    expect(assembled).toHaveLength(2);
    expect(assembled[0]).toHaveProperty('name', 'Alice');
    expect(assembled[0]).toHaveProperty('posts');
    expect(assembled[0]?.posts).toEqual([{ title: 'Post A1' }, { title: 'Post A2' }]);
    expect(assembled[1]).toHaveProperty('name', 'Bob');
    expect(assembled[1]?.posts).toEqual([{ title: 'Post B1' }]);
  });

  it('removes internal parent key aliases from output', () => {
    const mainRows = [{ __pk_posts: 1, name: 'Alice' }];

    const relationQueries = [
      {
        relationName: 'posts',
        parentKey: users.id,
        foreignKeyAlias: '__fk',
        mode: 'many' as const,
        query: new QuerySpy(),
      },
    ];

    const assembled = assembleDrizzleRelations(mainRows, relationQueries, [[]]);

    expect(assembled[0]).not.toHaveProperty('__pk_posts');
    expect(assembled[0]).toHaveProperty('posts', []);
    expect(assembled[0]).toHaveProperty('name', 'Alice');
  });

  it('removes internal foreign key alias from child rows', () => {
    const mainRows = [{ __pk_posts: 1, name: 'Alice' }];

    const relationQueries = [
      {
        relationName: 'posts',
        parentKey: users.id,
        foreignKeyAlias: '__fk',
        mode: 'many' as const,
        query: new QuerySpy(),
      },
    ];

    const relationResults = [[{ __fk: 1, title: 'Hello', id: 10 }]];

    const assembled = assembleDrizzleRelations(mainRows, relationQueries, relationResults);
    const firstRow = assembled[0];
    const postsArr = firstRow?.posts;
    const firstPost = Array.isArray(postsArr) ? postsArr[0] : undefined;
    expect(firstPost).toBeDefined();
    expect(firstPost).toEqual({ title: 'Hello', id: 10 });
    expect(firstPost).not.toHaveProperty('__fk');
  });

  it('returns empty array for parents with no matching children', () => {
    const mainRows = [
      { __pk_posts: 1, name: 'Alice' },
      { __pk_posts: 99, name: 'Nobody' },
    ];

    const relationQueries = [
      {
        relationName: 'posts',
        parentKey: users.id,
        foreignKeyAlias: '__fk',
        mode: 'many' as const,
        query: new QuerySpy(),
      },
    ];

    const relationResults = [[{ __fk: 1, title: 'Post A1' }]];

    const assembled = assembleDrizzleRelations(mainRows, relationQueries, relationResults);

    expect(assembled[0]?.posts).toEqual([{ title: 'Post A1' }]);
    expect(assembled[1]?.posts).toEqual([]);
  });

  it('throws when relationQueries and relationResults length mismatch', () => {
    expect(() =>
      assembleDrizzleRelations(
        [],
        [
          {
            relationName: 'posts',
            parentKey: users.id,
            foreignKeyAlias: '__fk',
            mode: 'many' as const,
            query: new QuerySpy(),
          },
        ],
        [],
      ),
    ).toThrow('Mismatch');
  });

  it('handles multiple relations at once', () => {
    const mainRows = [{ __pk_posts: 1, __pk_comments: 1, name: 'Alice' }];

    const relationQueries = [
      {
        relationName: 'posts',
        parentKey: users.id,
        foreignKeyAlias: '__fk',
        mode: 'many' as const,
        query: new QuerySpy(),
      },
      {
        relationName: 'comments',
        parentKey: users.id,
        foreignKeyAlias: '__fk',
        mode: 'many' as const,
        query: new QuerySpy(),
      },
    ];

    const relationResults = [
      [{ __fk: 1, title: 'Post 1' }],
      [
        { __fk: 1, body: 'Comment 1' },
        { __fk: 1, body: 'Comment 2' },
      ],
    ];

    const assembled = assembleDrizzleRelations(mainRows, relationQueries, relationResults);

    expect(assembled[0]).not.toHaveProperty('__pk_posts');
    expect(assembled[0]).not.toHaveProperty('__pk_comments');
    expect(assembled[0]?.posts).toEqual([{ title: 'Post 1' }]);
    expect(assembled[0]?.comments).toEqual([{ body: 'Comment 1' }, { body: 'Comment 2' }]);
  });

  it('supports composite foreign keys (array of columns)', () => {
    const mainRows = [
      { __pk_items_0: 1, __pk_items_1: 100, name: 'Order A' },
      { __pk_items_0: 2, __pk_items_1: 200, name: 'Order B' },
    ];

    const relationQueries = [
      {
        relationName: 'items',
        parentKey: [users.id, users.name], // stand-in columns for test
        foreignKeyAlias: ['__fk_0', '__fk_1'],
        mode: 'many' as const,
        query: new QuerySpy(),
      },
    ];

    const relationResults = [
      [
        { __fk_0: 1, __fk_1: 100, qty: 3 },
        { __fk_0: 1, __fk_1: 100, qty: 5 },
        { __fk_0: 2, __fk_1: 200, qty: 1 },
      ],
    ];

    const assembled = assembleDrizzleRelations(mainRows, relationQueries, relationResults);

    expect(assembled).toHaveLength(2);
    // Order A has 2 items
    expect(assembled[0]).toHaveProperty('name', 'Order A');
    expect(assembled[0]?.items).toEqual([{ qty: 3 }, { qty: 5 }]);
    // Order B has 1 item
    expect(assembled[1]).toHaveProperty('name', 'Order B');
    expect(assembled[1]?.items).toEqual([{ qty: 1 }]);
    // Internal aliases removed
    expect(assembled[0]).not.toHaveProperty('__pk_items_0');
    expect(assembled[0]).not.toHaveProperty('__pk_items_1');
  });

  it('composite FK returns empty array when no children match', () => {
    const mainRows = [{ __pk_items_0: 99, __pk_items_1: 99, name: 'Orphan' }];

    const relationQueries = [
      {
        relationName: 'items',
        parentKey: [users.id, users.name],
        foreignKeyAlias: ['__fk_0', '__fk_1'],
        mode: 'many' as const,
        query: new QuerySpy(),
      },
    ];

    const assembled = assembleDrizzleRelations(mainRows, relationQueries, [[]]);

    expect(assembled[0]).toHaveProperty('name', 'Orphan');
    expect(assembled[0]?.items).toEqual([]);
  });

  it('composite FK removes internal aliases from child rows', () => {
    const mainRows = [{ __pk_items_0: 1, __pk_items_1: 2, name: 'X' }];

    const relationQueries = [
      {
        relationName: 'items',
        parentKey: [users.id, users.name],
        foreignKeyAlias: ['__fk_0', '__fk_1'],
        mode: 'many' as const,
        query: new QuerySpy(),
      },
    ];

    const relationResults = [[{ __fk_0: 1, __fk_1: 2, qty: 10, price: 50 }]];

    const assembled = assembleDrizzleRelations(mainRows, relationQueries, relationResults);

    const firstItem = Array.isArray(assembled[0]?.items) ? assembled[0].items[0] : undefined;
    expect(firstItem).toEqual({ qty: 10, price: 50 });
    expect(firstItem).not.toHaveProperty('__fk_0');
    expect(firstItem).not.toHaveProperty('__fk_1');
  });

  it('returns single object for mode "one" when child exists', () => {
    const mainRows = [
      { __pk_profile: 1, name: 'Alice' },
      { __pk_profile: 2, name: 'Bob' },
    ];

    const relationQueries = [
      {
        relationName: 'profile',
        parentKey: users.id,
        foreignKeyAlias: '__fk',
        mode: 'one' as const,
        query: new QuerySpy(),
      },
    ];

    const relationResults = [[{ __fk: 1, bio: 'Hello' }]];

    const assembled = assembleDrizzleRelations(mainRows, relationQueries, relationResults);

    expect(assembled[0]?.profile).toEqual({ bio: 'Hello' });
    expect(assembled[1]?.profile).toBeNull();
  });

  it('returns null for mode "one" when no child matches', () => {
    const mainRows = [{ __pk_profile: 1, name: 'Alice' }];

    const relationQueries = [
      {
        relationName: 'profile',
        parentKey: users.id,
        foreignKeyAlias: '__fk',
        mode: 'one' as const,
        query: new QuerySpy(),
      },
    ];

    const assembled = assembleDrizzleRelations(mainRows, relationQueries, [[]]);

    expect(assembled[0]?.profile).toBeNull();
  });

  it('returns null for mode "one" when parent key is invalid', () => {
    const mainRows = [{ name: 'Alice' }];

    const relationQueries = [
      {
        relationName: 'profile',
        parentKey: users.id,
        foreignKeyAlias: '__fk',
        mode: 'one' as const,
        query: new QuerySpy(),
      },
    ];

    const assembled = assembleDrizzleRelations(mainRows, relationQueries, [[]]);

    expect(assembled[0]?.profile).toBeNull();
  });

  it('returns first child only for mode "one" when multiple children exist', () => {
    const mainRows = [{ __pk_profile: 1, name: 'Alice' }];

    const relationQueries = [
      {
        relationName: 'profile',
        parentKey: users.id,
        foreignKeyAlias: '__fk',
        mode: 'one' as const,
        query: new QuerySpy(),
      },
    ];

    const relationResults = [
      [
        { __fk: 1, bio: 'First' },
        { __fk: 1, bio: 'Second' },
      ],
    ];

    const assembled = assembleDrizzleRelations(mainRows, relationQueries, relationResults);

    expect(assembled[0]?.profile).toEqual({ bio: 'First' });
  });

  it('defaults to mode "many" when mode is not specified', () => {
    const mainRows = [{ __pk_posts: 1, name: 'Alice' }];

    const relationQueries = [
      {
        relationName: 'posts',
        parentKey: users.id,
        foreignKeyAlias: '__fk',
        mode: 'many' as const,
        query: new QuerySpy(),
      },
    ];

    const relationResults = [[{ __fk: 1, title: 'Post A' }]];

    const assembled = assembleDrizzleRelations(mainRows, relationQueries, relationResults);

    expect(assembled[0]?.posts).toEqual([{ title: 'Post A' }]);
  });

  it('applies per-parent limit when limit is set', () => {
    const mainRows = [
      { __pk_posts: 1, name: 'Alice' },
      { __pk_posts: 2, name: 'Bob' },
    ];

    const relationQueries = [
      {
        relationName: 'posts',
        parentKey: users.id,
        foreignKeyAlias: '__fk',
        mode: 'many' as const,
        limit: 2,
        query: new QuerySpy(),
      },
    ];

    const relationResults = [
      [
        { __fk: 1, title: 'A1' },
        { __fk: 1, title: 'A2' },
        { __fk: 1, title: 'A3' },
        { __fk: 2, title: 'B1' },
      ],
    ];

    const assembled = assembleDrizzleRelations(mainRows, relationQueries, relationResults);

    // Alice has 3 children but only 2 kept
    expect(assembled[0]?.posts).toEqual([{ title: 'A1' }, { title: 'A2' }]);
    // Bob has 1 child — all kept (under limit)
    expect(assembled[1]?.posts).toEqual([{ title: 'B1' }]);
  });

  it('does not apply limit when limit is undefined', () => {
    const mainRows = [{ __pk_posts: 1, name: 'Alice' }];

    const relationQueries = [
      {
        relationName: 'posts',
        parentKey: users.id,
        foreignKeyAlias: '__fk',
        mode: 'many' as const,
        query: new QuerySpy(),
      },
    ];

    const relationResults = [
      [
        { __fk: 1, title: 'A1' },
        { __fk: 1, title: 'A2' },
        { __fk: 1, title: 'A3' },
      ],
    ];

    const assembled = assembleDrizzleRelations(mainRows, relationQueries, relationResults);

    expect(assembled[0]?.posts).toHaveLength(3);
  });
});

describe('buildLimitOffsetResponseMeta', () => {
  function toLimitOffsetParsed(
    pagination: PaginationPayload<TestSchema, 'LIMIT_OFFSET'>,
  ): PaginationPayload<TestSchema, 'LIMIT_OFFSET'> {
    return pagination;
  }

  it('computes totalPages and currentPage', () => {
    const parsed = toLimitOffsetParsed({
      type: 'LIMIT_OFFSET',
      page: 2,
      limit: 10,
    });

    const meta = buildLimitOffsetResponseMeta(parsed, 42);

    expect(meta.itemsPerPage).toBe(10);
    expect(meta.totalItems).toBe(42);
    expect(meta.currentPage).toBe(2);
    expect(meta.totalPages).toBe(5);
  });

  it('returns totalPages 1 when totalItems is 0', () => {
    const parsed = toLimitOffsetParsed({
      type: 'LIMIT_OFFSET',
      page: 1,
      limit: 10,
    });

    const meta = buildLimitOffsetResponseMeta(parsed, 0);

    expect(meta.totalPages).toBe(1);
    expect(meta.totalItems).toBe(0);
    expect(meta.currentPage).toBe(1);
  });

  it('normalizes page to 1 when page is 0 or undefined', () => {
    const parsed = toLimitOffsetParsed({
      type: 'LIMIT_OFFSET',
      page: 0,
      limit: 5,
    });

    const meta = buildLimitOffsetResponseMeta(parsed, 20);

    expect(meta.currentPage).toBe(1);
  });

  it('includes sortBy and filter when present', () => {
    const parsed = toLimitOffsetParsed({
      type: 'LIMIT_OFFSET',
      page: 1,
      limit: 10,
      sortBy: [{ property: 'name', direction: 'ASC' }],
      filters: {
        type: 'filter',
        field: 'name',
        condition: { group: 'name', op: '$eq', value: 'alice' },
      },
    });

    const meta = buildLimitOffsetResponseMeta(parsed, 1);

    expect(meta.sortBy).toEqual([{ property: 'name', direction: 'ASC' }]);
    expect(meta.filter).toBeDefined();
  });
});

describe('buildCursorResponseMeta', () => {
  function toCursorParsed(
    pagination: PaginationPayload<TestSchema, 'CURSOR'>,
  ): PaginationPayload<TestSchema, 'CURSOR'> {
    return pagination;
  }

  it('extracts cursor from last row', () => {
    const parsed = toCursorParsed({
      type: 'CURSOR',
      limit: 10,
      cursor: 5,
      cursorProperty: 'id',
    });

    const rows = [{ id: 6 }, { id: 7 }, { id: 8 }];

    const meta = buildCursorResponseMeta(parsed, rows, 'id');

    expect(meta.itemsPerPage).toBe(10);
    expect(meta.cursor).toBe(8);
  });

  it('uses incoming cursor when rows are empty', () => {
    const parsed = toCursorParsed({
      type: 'CURSOR',
      limit: 10,
      cursor: 42,
      cursorProperty: 'id',
    });

    const meta = buildCursorResponseMeta(parsed, [], 'id');

    expect(meta.cursor).toBe(42);
  });

  it('falls back to 0 when no cursor and no rows', () => {
    const parsed = toCursorParsed({
      type: 'CURSOR',
      limit: 10,
      cursorProperty: 'id',
    });

    const meta = buildCursorResponseMeta(parsed, [], 'id');

    expect(meta.cursor).toBe(0);
  });

  it('handles string cursor values', () => {
    const parsed = toCursorParsed({
      type: 'CURSOR',
      limit: 5,
      cursorProperty: 'name',
    });

    const rows = [{ name: 'Alice' }, { name: 'Bob' }];

    const meta = buildCursorResponseMeta(parsed, rows, 'name');

    expect(meta.cursor).toBe('Bob');
  });

  it('includes sortBy and filter when present', () => {
    const parsed = toCursorParsed({
      type: 'CURSOR',
      limit: 10,
      cursorProperty: 'id',
      sortBy: [{ property: 'id', direction: 'DESC' }],
      filters: {
        type: 'filter',
        field: 'name',
        condition: { group: 'name', op: '$eq', value: 'alice' },
      },
    });

    const meta = buildCursorResponseMeta(parsed, [{ id: 1 }], 'id');

    expect(meta.sortBy).toEqual([{ property: 'id', direction: 'DESC' }]);
    expect(meta.filter).toBeDefined();
  });
});
