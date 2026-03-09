# zod-paginate-aws-s3

AWS S3 adapter for [`zod-paginate`](https://github.com/nolway/zod-paginate).

This library is **not standalone** — it requires [`zod-paginate`](https://github.com/nolway/zod-paginate) as a base. `zod-paginate` handles schema definition, query parsing, and validation; this package provides the S3 integration to list, filter, sort and paginate objects from an S3 bucket **in-memory**.

## Features

- Lists all objects from an S3 bucket via `ListObjectsV2Command` with automatic cursor-based pagination
- Generic adapter: you provide a mapping function (`_Object` -> your type `T`) and a field resolver
- Evaluates filter trees (`and` / `or` / `filter`) in-memory on mapped objects
- Supports string operators (`$eq`, `$contains`, `$in`, `$ilike`, `$sw`, `$null`) and comparison operators (`$gt`, `$gte`, `$lt`, `$lte`, `$btw`) with `not` modifier
- Applies multi-field sorting (ASC / DESC) with numeric and string comparison
- Handles limit/offset pagination in-memory and returns page metadata

## Installation

```bash
npm install zod-paginate-aws-s3
```

You will also need the following peer dependencies:

- `@aws-sdk/client-s3` (v3)
- `zod-paginate`

## Quick start

```ts
import { S3Client, type _Object } from '@aws-sdk/client-s3';
import { paginateS3Objects } from 'zod-paginate-aws-s3';

// 1. Define your domain type
interface MyFile {
  name: string;
  extension: string;
  size: number;
}

// 2. Provide a mapper from S3 _Object to your type (return null to skip)
function mapObject(obj: _Object): MyFile | null {
  if (!obj.Key || obj.Key.endsWith('/')) return null;
  const fileName = obj.Key.split('/').at(-1) ?? '';
  const dotIndex = fileName.lastIndexOf('.');
  return {
    name: dotIndex > 0 ? fileName.slice(0, dotIndex) : fileName,
    extension: dotIndex > 0 ? fileName.slice(dotIndex + 1) : '',
    size: obj.Size ?? 0,
  };
}

// 3. Provide a field resolver so the adapter can filter and sort on your fields
function resolveField(item: MyFile, field: string): string | undefined {
  const lookup: Record<string, string | undefined> = {
    name: item.name,
    extension: item.extension,
    size: String(item.size),
  };
  return lookup[field];
}

// 4. Call paginateS3Objects
const result = await paginateS3Objects(
  {
    limit: 20,
    page: 2,
    sortBy: [{ property: 'name', direction: 'ASC' }],
    filters: {
      type: 'filter',
      field: 'extension',
      condition: { group: 'extension', op: '$eq', value: 'pdf' },
    },
  },
  {
    client: new S3Client({ region: 'eu-west-1' }),
    bucket: 'my-bucket',
    prefix: 'uploads/',
    mapObject,
    resolveField,
  },
);

// result.data          → MyFile[] (current page)
// result.pagination    → { itemsPerPage, totalItems, currentPage, totalPages, sortBy?, filter? }
```

## API

### `paginateS3Objects<T>(input, config)`

Fetches all S3 objects from the configured bucket, maps them to your type `T`, then applies filtering, sorting and pagination in-memory.

**Input** (`S3PaginateInput`):

| Field     | Type           | Description                          |
| --------- | -------------- | ------------------------------------ |
| `limit`   | `number`       | Items per page (`0` = no pagination) |
| `page`    | `number?`      | Current page (defaults to `1`)       |
| `sortBy`  | `SortClause[]?`| Multi-field sort directives          |
| `filters` | `WhereNode?`   | Filter tree from `zod-paginate`      |

**Config** (`S3PaginateConfig<T>`):

| Field          | Type                                       | Description                                                |
| -------------- | ------------------------------------------ | ---------------------------------------------------------- |
| `client`       | `S3Client`                                 | An initialized `@aws-sdk/client-s3` client                 |
| `bucket`       | `string`                                   | S3 bucket name                                             |
| `prefix`       | `string?`                                  | Optional key prefix to scope the listing                   |
| `mapObject`    | `(obj: _Object) => T \| null`              | Maps an S3 object to your domain type (return `null` to skip) |
| `resolveField` | `(item: T, field: string) => string \| undefined` | Extracts a field value from a mapped item for filtering/sorting |

**Returns** (`S3PaginateResult<T>`):

| Field                    | Type           | Description                |
| ------------------------ | -------------- | -------------------------- |
| `data`                   | `T[]`          | Items for the current page |
| `pagination.itemsPerPage`| `number`       | Requested limit            |
| `pagination.totalItems`  | `number`       | Total matching items       |
| `pagination.currentPage` | `number`       | Current page number        |
| `pagination.totalPages`  | `number`       | Total number of pages      |
| `pagination.sortBy`      | `SortClause[]?`| Applied sort directives    |
| `pagination.filter`      | `WhereNode?`   | Applied filter tree        |

### `fetchAllS3Objects(client, bucket, prefix?)`

Low-level helper that lists **all** objects from a bucket (handling `ContinuationToken` automatically). Returns `_Object[]`.

### `evaluateWhereNode<T>(item, node, resolveField)`

Evaluates a `WhereNode` filter tree against a single item. Useful if you want to apply `zod-paginate` filters on your own data outside of the S3 flow.

### `applySort<T>(items, resolveField, sortBy?)`

Sorts an array of items using the same multi-field sort logic. Supports numeric and string comparison.

## Supported filter operators

| Operator    | Description                                      |
| ----------- | ------------------------------------------------ |
| `$null`     | Field is `undefined`                             |
| `$eq`       | Exact equality                                   |
| `$in`       | Field value is in the given array                |
| `$contains` | Case-insensitive check: field includes any value |
| `$ilike`    | Case-insensitive substring match                 |
| `$sw`       | Case-insensitive starts-with                     |
| `$gt`       | Greater than                                     |
| `$gte`      | Greater than or equal                            |
| `$lt`       | Less than                                        |
| `$lte`      | Less than or equal                               |
| `$btw`      | Between (inclusive, `[min, max]`)                 |

All operators support the `not` modifier to negate the condition.

## License

[GPL-3.0](./LICENSE.txt)
