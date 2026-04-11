import { eq, sql } from 'drizzle-orm';
import { describe, expect, it } from 'vitest';
import type { DataSchema, PaginationPayload, SelectQueryPayload } from 'zod-paginate';
import {
  applyDrizzlePaginationOnQuery,
  defineRelation,
  generatePaginationQuery,
  generateSelectQuery,
} from '../../src/drizzle-adapter';
import { users, posts } from './schemas';
import { db, seedUsers, setupMysql } from './setup';

function toParsed(pagination: PaginationPayload<DataSchema>): PaginationPayload<DataSchema> {
  return pagination;
}

describe('MySQL integration', () => {
  setupMysql();

  const fields = {
    id: users.id,
    name: users.name,
    email: users.email,
    age: users.age,
    status: users.status,
  };

  it('returns paginated results with limit and offset', async () => {
    await seedUsers();

    const parsed = toParsed({
      type: 'LIMIT_OFFSET',
      page: 1,
      limit: 3,
      select: ['id', 'name', 'email'],
      sortBy: [{ property: 'id', direction: 'ASC' }],
    });

    const { query } = applyDrizzlePaginationOnQuery(parsed, {
      dialect: 'mysql',
      fields,
      buildQuery: (select) => db.select(select).from(users),
    });

    const rows = await query;

    expect(rows).toHaveLength(3);
    expect(rows[0]).toHaveProperty('name', 'Alice');
    expect(rows[2]).toHaveProperty('name', 'Charlie');
  });

  it('returns second page correctly', async () => {
    await seedUsers();

    const parsed = toParsed({
      type: 'LIMIT_OFFSET',
      page: 2,
      limit: 3,
      select: ['id', 'name'],
      sortBy: [{ property: 'id', direction: 'ASC' }],
    });

    const { query } = applyDrizzlePaginationOnQuery(parsed, {
      dialect: 'mysql',
      fields,
      buildQuery: (select) => db.select(select).from(users),
    });

    const rows = await query;

    expect(rows).toHaveLength(2);
    expect(rows[0]).toHaveProperty('name', 'Diana');
    expect(rows[1]).toHaveProperty('name', 'Eve');
  });

  it('filters with $eq operator', async () => {
    await seedUsers();

    const parsed = toParsed({
      type: 'LIMIT_OFFSET',
      page: 1,
      limit: 10,
      select: ['name', 'status'],
      filters: {
        type: 'filter',
        field: 'status',
        condition: { group: 'status', op: '$eq', value: 'ACTIVE' },
      },
    });

    const { query } = applyDrizzlePaginationOnQuery(parsed, {
      dialect: 'mysql',
      fields,
      buildQuery: (select) => db.select(select).from(users),
    });

    const rows = await query;

    expect(rows).toHaveLength(3);
    expect(rows.every((r: Record<string, unknown>) => r.status === 'ACTIVE')).toBe(true);
  });

  it('filters with $in operator', async () => {
    await seedUsers();

    const parsed = toParsed({
      type: 'LIMIT_OFFSET',
      page: 1,
      limit: 10,
      select: ['name'],
      filters: {
        type: 'filter',
        field: 'name',
        condition: { group: 'name', op: '$in', value: ['Alice', 'Eve'] },
      },
    });

    const { query } = applyDrizzlePaginationOnQuery(parsed, {
      dialect: 'mysql',
      fields,
      buildQuery: (select) => db.select(select).from(users),
    });

    const rows = await query;

    expect(rows).toHaveLength(2);
  });

  it('filters with $btw operator', async () => {
    await seedUsers();

    const parsed = toParsed({
      type: 'LIMIT_OFFSET',
      page: 1,
      limit: 10,
      select: ['name', 'age'],
      filters: {
        type: 'filter',
        field: 'age',
        condition: { group: 'age', op: '$btw', value: [25, 30] },
      },
    });

    const { query } = applyDrizzlePaginationOnQuery(parsed, {
      dialect: 'mysql',
      fields,
      buildQuery: (select) => db.select(select).from(users),
    });

    const rows = await query;

    expect(rows).toHaveLength(3); // Bob(25), Diana(28), Alice(30)
  });

  it('filters with $ilike (mapped to LIKE for MySQL)', async () => {
    await seedUsers();

    const parsed = toParsed({
      type: 'LIMIT_OFFSET',
      page: 1,
      limit: 10,
      select: ['name'],
      filters: {
        type: 'filter',
        field: 'name',
        condition: { group: 'name', op: '$ilike', value: 'ali' },
      },
    });

    const { query } = applyDrizzlePaginationOnQuery(parsed, {
      dialect: 'mysql',
      fields,
      buildQuery: (select) => db.select(select).from(users),
    });

    const rows = await query;

    expect(rows).toHaveLength(1);
    expect(rows[0]).toHaveProperty('name', 'Alice');
  });

  it('filters with $sw (starts with)', async () => {
    await seedUsers();

    const parsed = toParsed({
      type: 'LIMIT_OFFSET',
      page: 1,
      limit: 10,
      select: ['name'],
      filters: {
        type: 'filter',
        field: 'name',
        condition: { group: 'name', op: '$sw', value: 'Ch' },
      },
    });

    const { query } = applyDrizzlePaginationOnQuery(parsed, {
      dialect: 'mysql',
      fields,
      buildQuery: (select) => db.select(select).from(users),
    });

    const rows = await query;

    expect(rows).toHaveLength(1);
    expect(rows[0]).toHaveProperty('name', 'Charlie');
  });

  it('filters with $null operator', async () => {
    await seedUsers();
    await db.execute(
      sql`INSERT INTO users (name, email, status) VALUES ('NoAge', 'noage@test.com', 'ACTIVE')`,
    );

    const parsed = toParsed({
      type: 'LIMIT_OFFSET',
      page: 1,
      limit: 10,
      select: ['name'],
      filters: {
        type: 'filter',
        field: 'age',
        condition: { group: 'age', op: '$null' },
      },
    });

    const { query } = applyDrizzlePaginationOnQuery(parsed, {
      dialect: 'mysql',
      fields,
      buildQuery: (select) => db.select(select).from(users),
    });

    const rows = await query;

    expect(rows).toHaveLength(1);
    expect(rows[0]).toHaveProperty('name', 'NoAge');
  });

  it('filters with negation (not modifier)', async () => {
    await seedUsers();

    const parsed = toParsed({
      type: 'LIMIT_OFFSET',
      page: 1,
      limit: 10,
      select: ['name', 'status'],
      filters: {
        type: 'filter',
        field: 'status',
        condition: { group: 'status', op: '$eq', value: 'ACTIVE', not: true },
      },
    });

    const { query } = applyDrizzlePaginationOnQuery(parsed, {
      dialect: 'mysql',
      fields,
      buildQuery: (select) => db.select(select).from(users),
    });

    const rows = await query;

    expect(rows).toHaveLength(2);
    expect(rows.every((r: Record<string, unknown>) => r.status !== 'ACTIVE')).toBe(true);
  });

  it('combines OR filters', async () => {
    await seedUsers();

    const parsed = toParsed({
      type: 'LIMIT_OFFSET',
      page: 1,
      limit: 10,
      select: ['name', 'status'],
      filters: {
        type: 'or',
        items: [
          {
            type: 'filter',
            field: 'status',
            condition: { group: 'status', op: '$eq', value: 'BANNED' },
          },
          {
            type: 'filter',
            field: 'name',
            condition: { group: 'name', op: '$eq', value: 'Alice' },
          },
        ],
      },
    });

    const { query } = applyDrizzlePaginationOnQuery(parsed, {
      dialect: 'mysql',
      fields,
      buildQuery: (select) => db.select(select).from(users),
    });

    const rows = await query;

    expect(rows).toHaveLength(2);
  });

  it('sorts results in DESC order', async () => {
    await seedUsers();

    const parsed = toParsed({
      type: 'LIMIT_OFFSET',
      page: 1,
      limit: 10,
      select: ['name', 'age'],
      sortBy: [{ property: 'age', direction: 'DESC' }],
    });

    const { query } = applyDrizzlePaginationOnQuery(parsed, {
      dialect: 'mysql',
      fields,
      buildQuery: (select) => db.select(select).from(users),
    });

    const rows = await query;
    const ages = rows.map((r: Record<string, unknown>) => r.age);

    expect(ages).toEqual([35, 30, 28, 25, 22]);
  });

  it('returns only selected columns', async () => {
    await seedUsers();

    const parsed = toParsed({
      type: 'LIMIT_OFFSET',
      page: 1,
      limit: 1,
      select: ['name'],
      sortBy: [{ property: 'id', direction: 'ASC' }],
    });

    const { query } = applyDrizzlePaginationOnQuery(parsed, {
      dialect: 'mysql',
      fields,
      buildQuery: (select) => db.select(select).from(users),
    });

    const rows = await query;

    expect(rows).toHaveLength(1);
    const firstRow = rows[0];
    expect(firstRow).toBeDefined();
    if (firstRow) {
      expect(Object.keys(firstRow)).toEqual(['name']);
    }
    expect(rows[0]).toHaveProperty('name', 'Alice');
  });

  it('works with joins across tables', async () => {
    await seedUsers();

    await db.execute(sql`
      INSERT INTO posts (title, author_id) VALUES ('Post by Alice', 1), ('Post by Bob', 2)
    `);

    const joinFields = {
      userName: users.name,
      postTitle: posts.title,
    };

    const parsed = toParsed({
      type: 'LIMIT_OFFSET',
      page: 1,
      limit: 10,
      select: ['userName', 'postTitle'],
      sortBy: [{ property: 'postTitle', direction: 'ASC' }],
    });

    const { query } = applyDrizzlePaginationOnQuery(parsed, {
      dialect: 'mysql',
      fields: joinFields,
      buildQuery: (select) =>
        db.select(select).from(posts).innerJoin(users, eq(users.id, posts.authorId)),
    });

    const rows = await query;

    expect(rows).toHaveLength(2);
    expect(rows[0]).toHaveProperty('userName', 'Alice');
    expect(rows[0]).toHaveProperty('postTitle', 'Post by Alice');
  });

  it('returns empty array when no rows match', async () => {
    await seedUsers();

    const parsed = toParsed({
      type: 'LIMIT_OFFSET',
      page: 1,
      limit: 10,
      select: ['name'],
      filters: {
        type: 'filter',
        field: 'name',
        condition: { group: 'name', op: '$eq', value: 'Nobody' },
      },
    });

    const { query } = applyDrizzlePaginationOnQuery(parsed, {
      dialect: 'mysql',
      fields,
      buildQuery: (select) => db.select(select).from(users),
    });

    const rows = await query;

    expect(rows).toHaveLength(0);
  });

  it('works with select only (no filters, no sort)', async () => {
    await seedUsers();

    const parsed = toParsed({
      type: 'LIMIT_OFFSET',
      page: 1,
      limit: 10,
      select: ['name', 'email'],
    });

    const { query } = applyDrizzlePaginationOnQuery(parsed, {
      dialect: 'mysql',
      fields,
      buildQuery: (select) => db.select(select).from(users),
    });

    const rows = await query;

    expect(rows).toHaveLength(5);
    const firstRow = rows[0];
    expect(firstRow).toBeDefined();
    if (firstRow) {
      expect(Object.keys(firstRow).sort()).toEqual(['email', 'name']);
    }
  });

  it('combines AND filters', async () => {
    await seedUsers();

    const parsed = toParsed({
      type: 'LIMIT_OFFSET',
      page: 1,
      limit: 10,
      select: ['name', 'status', 'age'],
      filters: {
        type: 'and',
        items: [
          {
            type: 'filter',
            field: 'status',
            condition: { group: 'status', op: '$eq', value: 'ACTIVE' },
          },
          {
            type: 'filter',
            field: 'age',
            condition: { group: 'age', op: '$gte', value: 28 },
          },
        ],
      },
    });

    const { query } = applyDrizzlePaginationOnQuery(parsed, {
      dialect: 'mysql',
      fields,
      buildQuery: (select) => db.select(select).from(users),
    });

    const rows = await query;

    expect(rows).toHaveLength(2); // Alice(30,ACTIVE), Diana(28,ACTIVE)
    expect(rows.every((r: Record<string, unknown>) => r.status === 'ACTIVE')).toBe(true);
  });

  it('sorts by multiple columns', async () => {
    await seedUsers();

    const parsed = toParsed({
      type: 'LIMIT_OFFSET',
      page: 1,
      limit: 10,
      select: ['name', 'status', 'age'],
      sortBy: [
        { property: 'status', direction: 'ASC' },
        { property: 'age', direction: 'DESC' },
      ],
    });

    const { query } = applyDrizzlePaginationOnQuery(parsed, {
      dialect: 'mysql',
      fields,
      buildQuery: (select) => db.select(select).from(users),
    });

    const rows = await query;

    expect(rows).toHaveLength(5);
    // ACTIVE first (sorted by age DESC): Alice(30), Diana(28), Bob(25)
    // then BANNED: Eve(22)
    // then INACTIVE: Charlie(35)
    expect(rows.map((r: Record<string, unknown>) => r.name)).toEqual([
      'Alice',
      'Diana',
      'Bob',
      'Eve',
      'Charlie',
    ]);
  });

  it('returns all columns when no select is specified', async () => {
    await seedUsers();

    const parsed = toParsed({
      type: 'LIMIT_OFFSET',
      page: 1,
      limit: 1,
      sortBy: [{ property: 'id', direction: 'ASC' }],
    });

    const { query, clauses } = applyDrizzlePaginationOnQuery(parsed, {
      dialect: 'mysql',
      fields,
      buildQuery: (select) => {
        if (Object.keys(select).length > 0) {
          return db.select(select).from(users);
        }
        return db.select().from(users);
      },
    });

    expect(clauses.select).toEqual({});

    const rows = await query;

    expect(rows).toHaveLength(1);
    const firstRow = rows[0];
    expect(firstRow).toBeDefined();
    if (firstRow) {
      expect(Object.keys(firstRow)).toContain('name');
      expect(Object.keys(firstRow)).toContain('email');
      expect(Object.keys(firstRow)).toContain('id');
    }
  });

  it('execute() returns data with relations and correct pagination metadata', async () => {
    await seedUsers();

    await db.execute(sql`
      INSERT INTO posts (title, author_id) VALUES
        ('Post A1', 1), ('Post A2', 1), ('Post B1', 2)
    `);

    const parsed = toParsed({
      type: 'LIMIT_OFFSET',
      page: 1,
      limit: 3,
      select: ['id', 'name', 'posts.id', 'posts.title'],
      sortBy: [{ property: 'id', direction: 'ASC' }],
    });

    const result = generatePaginationQuery(parsed, {
      dialect: 'mysql',
      buildQuery: (select) => db.select(select).from(users),
      fields: { id: users.id, name: users.name },
      relations: [
        defineRelation({
          relationName: 'posts',
          fields: { id: posts.id, title: posts.title },
          foreignKey: posts.authorId,
          parentKey: users.id,
          buildQuery: (select) => db.select(select).from(posts),
        }),
      ],
    });

    const { data, pagination } = await result.execute();

    expect(data).toHaveLength(3);
    // Alice (id=1) has 2 posts
    expect(data[0]).toHaveProperty('name', 'Alice');
    expect(data[0]?.posts).toHaveLength(2);
    // Bob (id=2) has 1 post
    expect(data[1]).toHaveProperty('name', 'Bob');
    expect(data[1]?.posts).toHaveLength(1);
    // Charlie (id=3) has no posts
    expect(data[2]).toHaveProperty('name', 'Charlie');
    expect(data[2]?.posts).toHaveLength(0);

    // Pagination metadata: count derived from buildQuery automatically
    expect(pagination).toEqual(
      expect.objectContaining({
        itemsPerPage: 3,
        totalItems: 5,
        currentPage: 1,
        totalPages: 2,
      }),
    );
  });

  it('execute() applies filters to both data and count', async () => {
    await seedUsers();

    const parsed = toParsed({
      type: 'LIMIT_OFFSET',
      page: 1,
      limit: 10,
      select: ['id', 'name'],
      filters: {
        type: 'filter',
        field: 'status',
        condition: { group: 'status', op: '$eq', value: 'ACTIVE' },
      },
    });

    const result = generatePaginationQuery(parsed, {
      dialect: 'mysql',
      buildQuery: (select) => db.select(select).from(users),
      fields: { id: users.id, name: users.name, status: users.status },
      relations: [],
    });

    const { data, pagination } = await result.execute();

    expect(data).toHaveLength(3);
    expect(pagination).toEqual(
      expect.objectContaining({
        totalItems: 3,
        totalPages: 1,
      }),
    );
  });

  it('generateSelectQuery with responseType "one" returns a single object', async () => {
    await seedUsers();

    const parsed: SelectQueryPayload<DataSchema> = {
      fields: ['id', 'name', 'email'],
      responseType: 'one',
    };

    const result = generateSelectQuery(parsed, {
      buildQuery: (select) => db.select(select).from(users),
      fields: { id: users.id, name: users.name, email: users.email },
    });

    const { data } = await result.execute();

    expect(data).not.toBeNull();
    expect(Array.isArray(data)).toBe(false);
    expect(data).toHaveProperty('name');
    expect(data).toHaveProperty('email');
  });

  it('generateSelectQuery with responseType "one" returns null when no rows match', async () => {
    // No seed — empty table

    const parsed: SelectQueryPayload<DataSchema> = {
      fields: ['id', 'name'],
      responseType: 'one',
    };

    const result = generateSelectQuery(parsed, {
      buildQuery: (select) => db.select(select).from(users),
      fields: { id: users.id, name: users.name },
    });

    const executeResult = await result.execute();

    expect(executeResult).toBeNull();
  });

  it('generateSelectQuery with responseType "one" and relations returns a single assembled object', async () => {
    await seedUsers();

    await db.execute(sql`
      INSERT INTO posts (title, author_id) VALUES ('Post A1', 1), ('Post A2', 1)
    `);

    const parsed: SelectQueryPayload<DataSchema> = {
      fields: ['id', 'name', 'posts.id', 'posts.title'],
      responseType: 'one',
    };

    const result = generateSelectQuery(parsed, {
      buildQuery: (select) => db.select(select).from(users),
      fields: { id: users.id, name: users.name },
      relations: [
        defineRelation({
          relationName: 'posts',
          fields: { id: posts.id, title: posts.title },
          foreignKey: posts.authorId,
          parentKey: users.id,
          buildQuery: (select) => db.select(select).from(posts),
        }),
      ],
    });

    const { data } = await result.execute();

    expect(data).not.toBeNull();
    expect(Array.isArray(data)).toBe(false);
    expect(data).toHaveProperty('name');
    expect(data).toHaveProperty('posts');
  });

  it('generateSelectQuery without responseType returns an array', async () => {
    await seedUsers();

    const parsed: SelectQueryPayload<DataSchema> = {
      fields: ['id', 'name'],
      responseType: 'many',
    };

    const result = generateSelectQuery(parsed, {
      buildQuery: (select) => db.select(select).from(users),
      fields: { id: users.id, name: users.name },
    });

    const { data } = await result.execute();

    expect(Array.isArray(data)).toBe(true);
    expect(data).toHaveLength(5);
  });
});
