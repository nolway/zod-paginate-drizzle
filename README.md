# zod-paginate-drizzle

Drizzle adapter for [`zod-paginate`](https://github.com/nolway/zod-paginate).

This library is **not standalone** — it requires [`zod-paginate`](https://github.com/nolway/zod-paginate) as a base. `zod-paginate` handles schema definition, query parsing, and validation; this package provides the Drizzle ORM integration to turn those parsed params into actual SQL clauses (`select`, `where`, `orderBy`, `limit`, `offset`).

## Features

- Works with parsed `PaginationQueryParams` from `zod-paginate`
- Supports PostgreSQL and MySQL dialect defaults
- Converts filter trees (`and` / `or`) into Drizzle expressions
- Handles field mapping with strict or permissive mode
- Supports generated `select` aliases with collision handling
- Includes reusable operator sets for PG and MySQL
- **Relations**: fetch related data (one-to-many) as separate queries and assemble them into parent rows
- **Type-safe pagination metadata**: return type is narrowed to `LimitOffsetPaginationResponseMeta` or `CursorPaginationResponseMeta` based on the parsed pagination type

## Installation

```bash
npm install zod-paginate-drizzle
```

You will also need:

- `drizzle-orm`
- `zod-paginate`
- `zod`

## Quick start

### Paginated query with `generatePaginationQuery`

```ts
import { generatePaginationQuery, defineRelation } from 'zod-paginate-drizzle';

// `parsed` is the validated output from zod-paginate
const parsed = {
  pagination: {
    type: 'LIMIT_OFFSET',
    page: 1,
    limit: 20,
    select: ['id', 'name', 'posts.id', 'posts.title'],
    sortBy: [{ property: 'id', direction: 'ASC' }],
    filters: {
      type: 'filter',
      field: 'status',
      condition: { group: 'status', op: '$eq', value: 'ACTIVE' },
    },
  },
};

const result = generatePaginationQuery(parsed, {
  dialect: 'pg',
  buildQuery: (select) => db.select(select).from(users),
  fields: { id: users.id, name: users.name, status: users.status },
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

// execute() runs the main query + all relation queries, assembles the result,
// and computes pagination metadata automatically
const { data, pagination } = await result.execute();

// data[0].posts is an array of { id, title }
// pagination is typed as LimitOffsetPaginationResponseMeta
// (narrowed from the 'LIMIT_OFFSET' type passed in parsed)
```

### Select-only query with `generateSelectQuery`

When you only need to select fields (no filters, sorting, or pagination), use `generateSelectQuery` with `SelectQueryParams` from `zod-paginate`:

```ts
import { generateSelectQuery, defineRelation } from 'zod-paginate-drizzle';

const parsed = {
  select: ['id', 'name', 'posts.title'],
};

const result = generateSelectQuery(parsed, {
  buildQuery: (select) => db.select(select).from(users),
  fields: { id: users.id, name: users.name },
  relations: [
    defineRelation({
      relationName: 'posts',
      fields: { title: posts.title },
      foreignKey: posts.authorId,
      parentKey: users.id,
      buildQuery: (select) => db.select(select).from(posts),
    }),
  ],
});

const { data } = await result.execute();
// data[0].posts is an array of { title }
```

#### Single-object response with `responseType: 'one'`

When `zod-paginate`'s `select()` is configured with `responseType: 'one'`, the parsed params include `responseType: 'one'`. `generateSelectQuery` automatically applies `LIMIT 1` and returns a single object (or `null`) instead of an array:

```ts
const parsed = {
  select: ['id', 'name', 'email'],
  responseType: 'one',  // ← from zod-paginate's select({ responseType: 'one' })
};

const result = generateSelectQuery(parsed, {
  buildQuery: (select) => db.select(select).from(users),
  fields: { id: users.id, name: users.name, email: users.email },
});

const { data } = await result.execute();
// data = { id: 1, name: 'Alice', email: 'alice@test.com' }
// or data = null if no row is found
```

When `responseType` is `'many'` (the default) or omitted, `data` remains an array as before.

### Without relations

`relations` is optional — simply omit it:

```ts
const result = generatePaginationQuery(parsed, {
  dialect: 'pg',
  buildQuery: (select) => db.select(select).from(users),
  fields: { id: users.id, name: users.name, email: users.email },
});

const { data, pagination } = await result.execute();
```

### Manual execution

If you need more control, you can use the lower-level properties instead of `execute()`:

```ts
const result = generatePaginationQuery(parsed, config);

// Execute queries yourself
const [mainRows, ...relationRows] = await Promise.all([
  result.query,
  ...result.relationQueries.map((r) => r.query),
]);

// Assemble relations manually
const data = result.assemble(mainRows, relationRows);

// Access raw clauses
result.clauses.where;   // SQL | undefined
result.clauses.orderBy; // SQL[]
result.clauses.limit;   // number | undefined
result.clauses.offset;  // number | undefined
```

## Working with joins

The `buildQuery` callback receives the generated select shape, so you build
your query (including joins) naturally:

```ts
const fields = {
  userName: users.name,
  postTitle: posts.title,
};

const result = generatePaginationQuery(parsed, {
  dialect: 'pg',
  fields,
  buildQuery: (select) =>
    db
      .select(select)
      .from(users)
      .leftJoin(posts, eq(users.id, posts.authorId)),
  relations: [],
});

const { data } = await result.execute();
```

## API

### `generatePaginationQuery(parsed, config)`

Main entry point. Builds a paginated query with optional relations. Returns a `DrizzlePaginationResult` object.

The return type is **narrowed based on the pagination type** in `parsed`:

```ts
// When parsed has type: 'LIMIT_OFFSET'
const result = generatePaginationQuery(parsed, config);
const { pagination } = await result.execute();
// pagination: LimitOffsetPaginationResponseMeta
//   → { totalItems, totalPages, currentPage, itemsPerPage }

// When parsed has type: 'CURSOR'
const result = generatePaginationQuery(parsed, config);
const { pagination } = await result.execute();
// pagination: CursorPaginationResponseMeta
//   → { itemsPerPage, cursor, sortBy, filter }
```

**Config** (`GeneratePaginationQueryConfig`):

| Option | Type | Description |
|---|---|---|
| `dialect` | `'pg' \| 'mysql'` | Database dialect |
| `buildQuery` | `(selectShape) => query` | Receives the generated select shape, returns a Drizzle query builder |
| `fields` | `Record<string, Column>` | Map from allowed field paths to Drizzle columns |
| `relations` | `DrizzleRelation[]` (optional) | Array of relations created with `defineRelation()` |
| `strictFieldMapping` | `boolean` (default `true`) | Throw when a requested field has no mapping |
| `selectAlias` | `(fieldPath: string) => string` | Custom alias generator (default: `a.b` → `a_b`) |
| `operators` | `DrizzleSqlOperatorSet` | Custom operator set (optional) |

**Returns** (`DrizzlePaginationResult`):

| Property | Type | Description |
|---|---|---|
| `query` | `DrizzleDynamicQuery` | The main Drizzle query (awaitable) with all clauses applied |
| `clauses` | `DrizzlePaginationClauses` | Generated clauses (`select`, `where`, `orderBy`, `limit`, `offset`) |
| `relationQueries` | `DrizzleRelationQuery[]` | Prepared relation queries (for manual execution) |
| `assemble` | `(mainRows, relationResults) => rows` | Manual row assembler |
| `execute` | `() => Promise<{ data, pagination }>` | Runs everything and returns assembled data + pagination metadata |

### `generateSelectQuery(parsed, config)`

Select-only counterpart of `generatePaginationQuery`. Works with `SelectQueryParams` from `zod-paginate` (only a `select` array — no filters, sorting, or pagination).

When `parsed.responseType` is `'one'`, the query is automatically limited to 1 row and `execute()` returns `{ data: T | null }` instead of `{ data: T[] }`.

**Config**:

| Option | Type | Description |
|---|---|---|
| `buildQuery` | `(selectShape) => query` | Receives the generated select shape, returns a Drizzle query builder |
| `fields` | `Record<string, Column>` | Map from allowed field paths to Drizzle columns |
| `relations` | `DrizzleRelation[]` (optional) | Array of relations created with `defineRelation()` |
| `strictFieldMapping` | `boolean` (default `true`) | Throw when a requested field has no mapping |
| `selectAlias` | `(fieldPath: string) => string` | Custom alias generator (default: `a.b` → `a_b`) |

**Returns** (`DrizzleSelectWithRelationsResult`):

| Property | Type | Description |
|---|---|---|
| `query` | `DrizzleDynamicQuery` | The main Drizzle query (awaitable) |
| `relationQueries` | `DrizzleRelationQuery[]` | Prepared relation queries |
| `assemble` | `(mainRows, relationResults) => rows` | Manual row assembler |
| `execute` | `() => Promise<{ data }>` | Runs everything and returns assembled data. When `responseType` is `'one'`: `data` is a single object or `null`. Otherwise: `data` is an array. |

### `defineRelation(config)`

Type-safe factory for creating relation definitions. Each relation describes a one-to-many (or one-to-one) relationship fetched as a separate query and assembled into parent rows.

```ts
const postsRelation = defineRelation({
  relationName: 'posts',        // key in the assembled result
  fields: {                     // column map for the child table
    id: posts.id,
    title: posts.title,
  },
  foreignKey: posts.authorId,   // child FK column(s)
  parentKey: users.id,          // parent PK column(s)
  buildQuery: (select) =>       // factory for child query
    db.select(select).from(posts),
});
```

#### One-to-one relations (`mode: 'one'`)

By default, relations are assembled as arrays (`mode: 'many'`). For one-to-one relations, set `mode: 'one'` to get a single object or `null` instead:

```ts
const profileRelation = defineRelation({
  relationName: 'profile',
  fields: { bio: profiles.bio, avatar: profiles.avatar },
  foreignKey: profiles.userId,
  parentKey: users.id,
  mode: 'one',                  // ← single object | null
  buildQuery: (select) => db.select(select).from(profiles),
});

const { data } = await result.execute();
// data[0].profile is { bio, avatar } | null  (not an array)
```

The return type is narrowed at the type level: `mode: 'one'` produces `T | null`, `mode: 'many'` (default) produces `T[]`.

#### Per-relation `orderBy` and `limit`

You can set a static `orderBy` and a per-parent `limit` directly on the relation definition. This is useful for "last N items" patterns:

```ts
import { desc } from 'drizzle-orm';

const recentPostsRelation = defineRelation({
  relationName: 'posts',
  fields: { id: posts.id, title: posts.title, createdAt: posts.createdAt },
  foreignKey: posts.authorId,
  parentKey: users.id,
  orderBy: [desc(posts.createdAt)],  // static sort (tiebreaker if client also sorts)
  limit: 5,                          // keep the 5 most recent posts per user
  buildQuery: (select) => db.select(select).from(posts),
});

const { data } = await result.execute();
// data[0].posts has at most 5 items, ordered by createdAt DESC
```

- `orderBy` is applied at the SQL level. If the client also requests sorting for this relation via query params (e.g. `sortBy=posts.title`), the client sort takes priority and the static order acts as a tiebreaker.
- `limit` is applied **per parent** during assembly (not at the SQL level). All matching children are fetched from the database; the limit caps how many are kept for each parent row.
- `limit` is ignored when `mode` is `'one'` (already capped at 1).

Composite foreign keys are supported using arrays:

```ts
defineRelation({
  relationName: 'items',
  fields: { id: items.id },
  foreignKey: [items.orderId, items.tenantId],
  parentKey: [orders.id, orders.tenantId],
  buildQuery: (select) => db.select(select).from(items),
});
```

### `buildLimitOffsetResponseMeta(parsed, totalItems)`

Computes limit/offset pagination metadata from parsed params and total count.

Returns `LimitOffsetPaginationResponseMeta`:

```ts
{ totalItems, totalPages, currentPage, itemsPerPage }
```

### `buildCursorResponseMeta(parsed, rows, cursorField?)`

Computes cursor pagination metadata from parsed params and result rows.

Returns `CursorPaginationResponseMeta`:

```ts
{ itemsPerPage, cursor, sortBy, filter }
```

### `createPgDrizzleOperators()`

Returns default PostgreSQL operators for Drizzle.

Includes support for `$contains` through Drizzle `arrayContains`.

### `createMySqlDrizzleOperators()`

Returns default MySQL operators for Drizzle.

`$ilike` and `$sw` map to `like` (collation decides case sensitivity).

## Supported filter operators

| Operator | Description |
|---|---|
| `$null` | Is null / is not null |
| `$eq` | Equals |
| `$in` | In array |
| `$contains` | Array contains (PG by default; custom for MySQL if needed) |
| `$gt` | Greater than |
| `$gte` | Greater than or equal |
| `$lt` | Less than |
| `$lte` | Less than or equal |
| `$btw` | Between (inclusive) |
| `$ilike` | Case-insensitive like |
| `$sw` | Starts with |

All operators support the `not` modifier for negation.

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
