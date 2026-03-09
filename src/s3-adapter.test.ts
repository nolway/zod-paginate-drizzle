import type { S3Client, _Object } from '@aws-sdk/client-s3';
import { describe, expect, it, vi } from 'vitest';
import type { Condition, WhereNode } from 'zod-paginate';
import {
  applySort,
  evaluateComparisonCondition,
  evaluateCondition,
  evaluateStringCondition,
  evaluateWhereNode,
  fetchAllS3Objects,
  paginateS3Objects,
} from './s3-adapter';

interface TestItem {
  name: string;
  size: number;
}

const resolveTestField = (item: TestItem, field: string): string | undefined => {
  if (field === 'name') return item.name;
  if (field === 'size') return String(item.size);
  return undefined;
};

function cond(op: string, value?: unknown, not?: boolean): Condition {
  // @ts-expect-error test helper creates conditions with intentionally broad types
  return { group: 'test', op, value, not };
}

function createMockS3Client(pages: _Object[][]): S3Client {
  let callIndex = 0;
  const send = vi.fn().mockImplementation(() => {
    const objects = pages[callIndex] ?? [];
    const hasMore = callIndex < pages.length - 1;
    callIndex++;
    return Promise.resolve({
      Contents: objects,
      IsTruncated: hasMore,
      NextContinuationToken: hasMore ? `token-${callIndex}` : undefined,
    });
  });
  // @ts-expect-error partial mock for testing
  return { send };
}

describe('evaluateStringCondition', () => {
  it('$null returns true when value is undefined', () => {
    expect(evaluateStringCondition(undefined, cond('$null'))).toBe(true);
  });

  it('$null returns false when value is defined', () => {
    expect(evaluateStringCondition('hello', cond('$null'))).toBe(false);
  });

  it('$eq matches exact string value', () => {
    expect(evaluateStringCondition('alice', cond('$eq', 'alice'))).toBe(true);
    expect(evaluateStringCondition('bob', cond('$eq', 'alice'))).toBe(false);
  });

  it('$contains checks case-insensitive inclusion', () => {
    expect(evaluateStringCondition('Hello World', cond('$contains', ['hello']))).toBe(true);
    expect(evaluateStringCondition('Hello World', cond('$contains', ['xyz']))).toBe(false);
  });

  it('$contains returns false for non-string field', () => {
    expect(evaluateStringCondition(undefined, cond('$contains', ['x']))).toBe(false);
  });

  it('$in checks membership in array', () => {
    expect(evaluateStringCondition('a', cond('$in', ['a', 'b', 'c']))).toBe(true);
    expect(evaluateStringCondition('z', cond('$in', ['a', 'b', 'c']))).toBe(false);
    expect(evaluateStringCondition(undefined, cond('$in', ['a']))).toBe(false);
  });

  it('$ilike performs case-insensitive contains', () => {
    expect(evaluateStringCondition('Alice Smith', cond('$ilike', 'alice'))).toBe(true);
    expect(evaluateStringCondition('Bob', cond('$ilike', 'alice'))).toBe(false);
  });

  it('$sw performs case-insensitive starts-with', () => {
    expect(evaluateStringCondition('Alice', cond('$sw', 'ali'))).toBe(true);
    expect(evaluateStringCondition('Bob', cond('$sw', 'ali'))).toBe(false);
  });

  it('throws on unsupported operator', () => {
    expect(() => evaluateStringCondition('x', cond('$unknown'))).toThrow('Unsupported operator');
  });
});

describe('evaluateComparisonCondition', () => {
  it('returns false when field is undefined', () => {
    expect(evaluateComparisonCondition(undefined, cond('$gt', 10))).toBe(false);
  });

  it('$gt compares numerically', () => {
    expect(evaluateComparisonCondition('15', cond('$gt', 10))).toBe(true);
    expect(evaluateComparisonCondition('5', cond('$gt', 10))).toBe(false);
  });

  it('$gte compares numerically', () => {
    expect(evaluateComparisonCondition('10', cond('$gte', 10))).toBe(true);
    expect(evaluateComparisonCondition('9', cond('$gte', 10))).toBe(false);
  });

  it('$lt compares numerically', () => {
    expect(evaluateComparisonCondition('5', cond('$lt', 10))).toBe(true);
    expect(evaluateComparisonCondition('15', cond('$lt', 10))).toBe(false);
  });

  it('$lte compares numerically', () => {
    expect(evaluateComparisonCondition('10', cond('$lte', 10))).toBe(true);
    expect(evaluateComparisonCondition('11', cond('$lte', 10))).toBe(false);
  });

  it('$btw checks numeric range', () => {
    expect(evaluateComparisonCondition('15', cond('$btw', [10, 20]))).toBe(true);
    expect(evaluateComparisonCondition('25', cond('$btw', [10, 20]))).toBe(false);
  });

  it('$btw checks string range when values are not numbers', () => {
    expect(evaluateComparisonCondition('b', cond('$btw', ['a', 'c']))).toBe(true);
    expect(evaluateComparisonCondition('z', cond('$btw', ['a', 'c']))).toBe(false);
  });

  it('returns false for NaN field values with numeric comparison', () => {
    expect(evaluateComparisonCondition('not-a-number', cond('$gt', 10))).toBe(false);
  });
});

describe('evaluateCondition', () => {
  it('dispatches string operators to evaluateStringCondition', () => {
    expect(evaluateCondition('hello', cond('$eq', 'hello'))).toBe(true);
  });

  it('dispatches comparison operators to evaluateComparisonCondition', () => {
    expect(evaluateCondition('15', cond('$gt', 10))).toBe(true);
  });

  it('returns true for unknown operators', () => {
    // @ts-expect-error testing unknown operator
    const exotic: Condition = { group: 'test', op: '$exotic' };
    expect(evaluateCondition('x', exotic)).toBe(true);
  });
});

describe('evaluateWhereNode', () => {
  const alice: TestItem = { name: 'alice', size: 100 };
  const bob: TestItem = { name: 'bob', size: 200 };

  it('evaluates a simple filter node', () => {
    const node: WhereNode = {
      type: 'filter',
      field: 'name',
      condition: cond('$eq', 'alice'),
    };
    expect(evaluateWhereNode(alice, node, resolveTestField)).toBe(true);
    expect(evaluateWhereNode(bob, node, resolveTestField)).toBe(false);
  });

  it('negates result when condition.not is true', () => {
    const node: WhereNode = {
      type: 'filter',
      field: 'name',
      condition: cond('$eq', 'alice', true),
    };
    expect(evaluateWhereNode(alice, node, resolveTestField)).toBe(false);
    expect(evaluateWhereNode(bob, node, resolveTestField)).toBe(true);
  });

  it('evaluates AND composite node', () => {
    const node: WhereNode = {
      type: 'and',
      items: [
        { type: 'filter', field: 'name', condition: cond('$eq', 'alice') },
        { type: 'filter', field: 'size', condition: cond('$gt', 50) },
      ],
    };
    expect(evaluateWhereNode(alice, node, resolveTestField)).toBe(true);
  });

  it('evaluates OR composite node', () => {
    const node: WhereNode = {
      type: 'or',
      items: [
        { type: 'filter', field: 'name', condition: cond('$eq', 'alice') },
        { type: 'filter', field: 'name', condition: cond('$eq', 'bob') },
      ],
    };
    expect(evaluateWhereNode(alice, node, resolveTestField)).toBe(true);
    expect(evaluateWhereNode(bob, node, resolveTestField)).toBe(true);
    const charlie: TestItem = { name: 'charlie', size: 0 };
    expect(evaluateWhereNode(charlie, node, resolveTestField)).toBe(false);
  });
});

describe('applySort', () => {
  const items: TestItem[] = [
    { name: 'charlie', size: 300 },
    { name: 'alice', size: 100 },
    { name: 'bob', size: 200 },
  ];

  it('returns items unchanged when no sortBy is provided', () => {
    expect(applySort(items, resolveTestField)).toBe(items);
  });

  it('sorts by string field ASC', () => {
    const sorted = applySort(items, resolveTestField, [{ property: 'name', direction: 'ASC' }]);
    expect(sorted.map((i) => i.name)).toEqual(['alice', 'bob', 'charlie']);
  });

  it('sorts by string field DESC', () => {
    const sorted = applySort(items, resolveTestField, [{ property: 'name', direction: 'DESC' }]);
    expect(sorted.map((i) => i.name)).toEqual(['charlie', 'bob', 'alice']);
  });

  it('sorts by numeric field ASC', () => {
    const sorted = applySort(items, resolveTestField, [{ property: 'size', direction: 'ASC' }]);
    expect(sorted.map((i) => i.size)).toEqual([100, 200, 300]);
  });

  it('sorts by numeric field DESC', () => {
    const sorted = applySort(items, resolveTestField, [{ property: 'size', direction: 'DESC' }]);
    expect(sorted.map((i) => i.size)).toEqual([300, 200, 100]);
  });

  it('handles null field values during sort', () => {
    const resolver = (item: TestItem, field: string): string | undefined => {
      if (field === 'name' && item.name === 'bob') return undefined;
      return resolveTestField(item, field);
    };
    const sorted = applySort(items, resolver, [{ property: 'name', direction: 'ASC' }]);
    expect(sorted.at(0)?.name).toBe('bob');
  });
});

describe('fetchAllS3Objects', () => {
  it('fetches all objects across multiple pages', async () => {
    const page1: _Object[] = [{ Key: 'a.txt' }, { Key: 'b.txt' }];
    const page2: _Object[] = [{ Key: 'c.txt' }];
    const client = createMockS3Client([page1, page2]);

    const result = await fetchAllS3Objects(client, 'my-bucket', 'prefix/');

    expect(result).toHaveLength(3);
    expect(result.map((o) => o.Key)).toEqual(['a.txt', 'b.txt', 'c.txt']);
  });

  it('returns empty array when bucket is empty', async () => {
    const client = createMockS3Client([[]]);
    const result = await fetchAllS3Objects(client, 'empty-bucket');
    expect(result).toEqual([]);
  });
});

describe('paginateS3Objects', () => {
  const s3Objects: _Object[] = [
    { Key: 'file-a.txt', Size: 100 },
    { Key: 'file-b.txt', Size: 200 },
    { Key: 'file-c.txt', Size: 300 },
    { Key: 'file-d.txt', Size: 400 },
    { Key: 'file-e.txt', Size: 500 },
  ];

  const mapObject = (obj: _Object): TestItem | null => {
    if (!obj.Key) return null;
    return { name: obj.Key, size: obj.Size ?? 0 };
  };

  const config = {
    client: createMockS3Client([s3Objects]),
    bucket: 'test-bucket',
    mapObject,
    resolveField: resolveTestField,
  };

  it('paginates results with limit and page', async () => {
    const result = await paginateS3Objects(
      { limit: 2, page: 1 },
      { ...config, client: createMockS3Client([s3Objects]) },
    );

    expect(result.data).toHaveLength(2);
    expect(result.pagination.totalItems).toBe(5);
    expect(result.pagination.totalPages).toBe(3);
    expect(result.pagination.currentPage).toBe(1);
    expect(result.pagination.itemsPerPage).toBe(2);
  });

  it('returns second page correctly', async () => {
    const result = await paginateS3Objects(
      { limit: 2, page: 2 },
      { ...config, client: createMockS3Client([s3Objects]) },
    );

    expect(result.data).toHaveLength(2);
    expect(result.data.at(0)?.name).toBe('file-c.txt');
    expect(result.data.at(1)?.name).toBe('file-d.txt');
    expect(result.pagination.currentPage).toBe(2);
  });

  it('applies filters', async () => {
    const result = await paginateS3Objects(
      {
        limit: 10,
        page: 1,
        filters: {
          type: 'filter',
          field: 'name',
          condition: cond('$eq', 'file-a.txt'),
        },
      },
      { ...config, client: createMockS3Client([s3Objects]) },
    );

    expect(result.data).toHaveLength(1);
    expect(result.data.at(0)?.name).toBe('file-a.txt');
  });

  it('applies sorting', async () => {
    const result = await paginateS3Objects(
      {
        limit: 10,
        page: 1,
        sortBy: [{ property: 'size', direction: 'DESC' }],
      },
      { ...config, client: createMockS3Client([s3Objects]) },
    );

    expect(result.data.at(0)?.size).toBe(500);
    expect(result.data.at(4)?.size).toBe(100);
  });

  it('returns all items when limit is 0', async () => {
    const result = await paginateS3Objects(
      { limit: 0, page: 1 },
      { ...config, client: createMockS3Client([s3Objects]) },
    );

    expect(result.data).toHaveLength(5);
    expect(result.pagination.totalPages).toBe(1);
  });
});
