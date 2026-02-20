# zod-paginate-drizzle

Drizzle adapter for `zod-paginate`.

Build Drizzle `select`, `where`, `orderBy`, `limit`, `offset` clauses from already-validated pagination params.

## Features

- Works with parsed `PaginationQueryParams` from `zod-paginate`
- Supports PostgreSQL and MySQL dialect defaults
- Converts filter trees (`and` / `or`) into Drizzle expressions
- Handles field mapping with strict or permissive mode
- Supports generated `select` aliases with collision handling
- Includes reusable operator sets for PG and MySQL

## Installation

```bash
npm install zod-paginate-drizzle
```

You will also need:

- `drizzle-orm`
- `zod-paginate`
- `zod`

## Quick start

```ts
import { applyDrizzlePaginationOnQuery } from 'zod-paginate-drizzle';

// `parsed` is the validated output from zod-paginate
const parsed = {
  pagination: {
    type: 'LIMIT_OFFSET',
    page: 2,
    limit: 20,
    select: ['id', 'email', 'status'],
    sortBy: [{ property: 'createdAt', direction: 'DESC' }],
    filters: {
      type: 'filter',
      field: 'status',
      condition: { group: 'status', op: '$eq', value: 'ACTIVE' },
    },
  },
};

const fields = {
  id: users.id,
  email: users.email,
  status: users.status,
  createdAt: users.createdAt,
};

const { query, clauses } = applyDrizzlePaginationOnQuery(parsed, {
  dialect: 'pg',
  fields,
  buildQuery: (select) => db.select(select).from(users),
});

// `query` is a real Drizzle query — just await it
const rows = await query;

// `clauses.select` is always a Record<string, Column> (never undefined)
```

## Working with joins

The `buildQuery` callback receives the generated select shape, so you build
your query (including joins) naturally:

```ts
const fields = {
  id: users.id,
  email: users.email,
  postTitle: posts.title,
  postCreatedAt: posts.createdAt,
};

const { query } = applyDrizzlePaginationOnQuery(parsed, {
  dialect: 'pg',
  fields,
  buildQuery: (select) =>
    db
      .select(select)
      .from(users)
      .leftJoin(posts, eq(users.id, posts.authorId)),
});

const rows = await query;
```

## API

### `applyDrizzlePaginationOnQuery(parsed, config)`

Builds the select shape and applies `where`, `orderBy`, `limit` and `offset` clauses.

Returns:

- `query`: a real Drizzle query (awaitable) returned by `buildQuery`, with clauses applied
- `clauses`: generated clauses:
  - `select`: `Record<string, Column>` (always defined, `{}` when empty)
  - `where`, `orderBy`, `limit`, `offset`

Config:

- `dialect`: `'pg' | 'mysql'`
- `buildQuery`: `(selectShape) => query` — receives the generated select shape, returns a Drizzle query builder. The adapter calls `.$dynamic()` internally
- `fields`: map from allowed field paths to Drizzle columns
- `strictFieldMapping` (default `true`): throw when a field has no mapping
- `selectAlias`: custom alias generator (default: `a.b` -> `a_b`)
- `operators`: custom operator set (optional)

### `createPgDrizzleOperators()`

Returns default PostgreSQL operators for Drizzle.

Includes support for `$contains` through Drizzle `arrayContains`.

### `createMySqlDrizzleOperators()`

Returns default MySQL operators for Drizzle.

`$ilike` and `$sw` map to `like` (collation decides case sensitivity).

## Supported filter operators

- `$null`
- `$eq`
- `$in`
- `$contains` (PG by default; custom for MySQL if needed)
- `$gt`
- `$gte`
- `$lt`
- `$lte`
- `$btw`
- `$ilike`
- `$sw`
- `not` modifier on each condition

## Important notes

### Empty select shape

When the parsed pagination has no `select`, the adapter passes `{}` to `buildQuery`. Calling `db.select({})` generates invalid SQL. Handle this in your callback:

```ts
buildQuery: (select) => {
  if (Object.keys(select).length > 0) {
    return db.select(select).from(users);
  }
  return db.select().from(users);
},
```
