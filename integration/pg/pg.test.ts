import { eq, sql } from 'drizzle-orm';
import { describe, expect, it } from 'vitest';
import type { DataSchema, PaginationQueryParams } from 'zod-paginate';
import { applyDrizzlePaginationOnQuery } from '../../src/drizzle-adapter';
import { posts, users } from './schemas';
import { db, seedUsers, setupPg } from './setup';

function toParsed(
  pagination: PaginationQueryParams<DataSchema>['pagination'],
): PaginationQueryParams<DataSchema> {
  return { pagination };
}

describe('PostgreSQL integration', () => {
  setupPg();

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
      dialect: 'pg',
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
      dialect: 'pg',
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
      dialect: 'pg',
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
      dialect: 'pg',
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
      dialect: 'pg',
      fields,
      buildQuery: (select) => db.select(select).from(users),
    });

    const rows = await query;

    expect(rows).toHaveLength(3); // Bob(25), Diana(28), Alice(30)
  });

  it('filters with $ilike operator', async () => {
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
      dialect: 'pg',
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
      dialect: 'pg',
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
      dialect: 'pg',
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
      dialect: 'pg',
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
      dialect: 'pg',
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
      dialect: 'pg',
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
      dialect: 'pg',
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
      dialect: 'pg',
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
      dialect: 'pg',
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
      dialect: 'pg',
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
      dialect: 'pg',
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
      dialect: 'pg',
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
      dialect: 'pg',
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
});
