import { ListObjectsV2Command, type S3Client, type _Object } from '@aws-sdk/client-s3';
import type { Condition, WhereNode } from 'zod-paginate';

export interface SortClause {
  property: string;
  direction: 'ASC' | 'DESC';
}

export interface S3PaginateInput {
  filters?: WhereNode;
  sortBy?: SortClause[];
  limit: number;
  page?: number;
}

export interface S3PaginateResult<T> {
  data: T[];
  pagination: {
    itemsPerPage: number;
    totalItems: number;
    currentPage: number;
    totalPages: number;
    sortBy?: SortClause[];
    filter?: WhereNode;
  };
}

export interface S3PaginateConfig<T> {
  client: S3Client;
  bucket: string;
  prefix?: string;
  mapObject: (obj: _Object) => T | null;
  resolveField: (item: T, field: string) => string | undefined;
}

export function evaluateStringCondition(
  fieldValue: string | undefined,
  condition: Condition,
): boolean {
  switch (condition.op) {
    case '$null':
      return fieldValue === undefined;
    case '$eq':
      return fieldValue === String(condition.value);
    case '$contains': {
      if (typeof fieldValue !== 'string' || !Array.isArray(condition.value)) return false;
      const lower = fieldValue.toLowerCase();
      return condition.value.some((v) => typeof v === 'string' && lower.includes(v.toLowerCase()));
    }
    case '$in':
      return (
        fieldValue !== undefined &&
        Array.isArray(condition.value) &&
        condition.value.includes(fieldValue)
      );
    case '$ilike':
      return (
        typeof fieldValue === 'string' &&
        typeof condition.value === 'string' &&
        fieldValue.toLowerCase().includes(condition.value.toLowerCase())
      );
    case '$sw':
      return (
        typeof fieldValue === 'string' &&
        typeof condition.value === 'string' &&
        fieldValue.toLowerCase().startsWith(condition.value.toLowerCase())
      );
    default:
      throw new Error(`Unsupported operator: ${JSON.stringify(condition)}`);
  }
}

export function evaluateComparisonCondition(
  fieldValue: string | undefined,
  condition: Condition,
): boolean {
  if (fieldValue === undefined || !('value' in condition)) return false;

  if (condition.op === '$btw') {
    const [min, max] = condition.value;
    if (typeof min === 'number' && typeof max === 'number') {
      const num = Number(fieldValue);
      return !Number.isNaN(num) && num >= min && num <= max;
    }
    return fieldValue >= String(min) && fieldValue <= String(max);
  }

  const compareAsNumber = typeof condition.value === 'number';
  const left = compareAsNumber ? Number(fieldValue) : fieldValue;
  const right = condition.value;

  if (compareAsNumber && Number.isNaN(left)) return false;

  switch (condition.op) {
    case '$gt':
      return left > right;
    case '$gte':
      return left >= right;
    case '$lt':
      return left < right;
    case '$lte':
      return left <= right;
    default:
      return true;
  }
}

export function evaluateCondition(fieldValue: string | undefined, condition: Condition): boolean {
  switch (condition.op) {
    case '$null':
    case '$eq':
    case '$contains':
    case '$in':
    case '$ilike':
    case '$sw':
      return evaluateStringCondition(fieldValue, condition);
    case '$gt':
    case '$gte':
    case '$lt':
    case '$lte':
    case '$btw':
      return evaluateComparisonCondition(fieldValue, condition);
    default:
      return true;
  }
}

export function evaluateWhereNode<T>(
  item: T,
  node: WhereNode,
  resolveField: (item: T, field: string) => string | undefined,
): boolean {
  switch (node.type) {
    case 'filter': {
      const fieldValue = resolveField(item, node.field);
      const result = evaluateCondition(fieldValue, node.condition);
      return node.condition.not ? !result : result;
    }
    case 'and':
      return node.items.every((child) => evaluateWhereNode(item, child, resolveField));
    case 'or':
      return node.items.some((child) => evaluateWhereNode(item, child, resolveField));
  }
}

export function applySort<T>(
  items: T[],
  resolveField: (item: T, field: string) => string | undefined,
  sortBy?: SortClause[],
): T[] {
  if (!sortBy || sortBy.length === 0) return items;

  return [...items].sort((a, b) => {
    for (const { property, direction } of sortBy) {
      const aVal = resolveField(a, property);
      const bVal = resolveField(b, property);

      if (aVal === bVal) continue;
      if (aVal == null) return direction === 'ASC' ? -1 : 1;
      if (bVal == null) return direction === 'ASC' ? 1 : -1;

      const aNum = Number(aVal);
      const bNum = Number(bVal);
      const cmp =
        !Number.isNaN(aNum) && !Number.isNaN(bNum)
          ? Math.sign(aNum - bNum)
          : aVal.localeCompare(bVal);
      if (cmp === 0) continue;
      return direction === 'ASC' ? cmp : -cmp;
    }
    return 0;
  });
}

export async function fetchAllS3Objects(
  client: S3Client,
  bucket: string,
  prefix?: string,
): Promise<_Object[]> {
  const objects: _Object[] = [];
  let continuationToken: string | undefined;
  let isTruncated = true;

  while (isTruncated) {
    const command = new ListObjectsV2Command({
      Bucket: bucket,
      Prefix: prefix ?? undefined,
      ContinuationToken: continuationToken,
    });

    // eslint-disable-next-line no-await-in-loop
    const response = await client.send(command);

    if (response.Contents) {
      objects.push(...response.Contents);
    }

    continuationToken = response.NextContinuationToken;
    isTruncated = response.IsTruncated ?? false;
  }

  return objects;
}

export async function paginateS3Objects<T>(
  input: S3PaginateInput,
  config: S3PaginateConfig<T>,
): Promise<S3PaginateResult<T>> {
  const s3Objects = await fetchAllS3Objects(config.client, config.bucket, config.prefix);

  const allItems = s3Objects.map(config.mapObject).filter((item): item is T => item !== null);

  const { filters } = input;
  const filtered = filters
    ? allItems.filter((item) => evaluateWhereNode(item, filters, config.resolveField))
    : allItems;

  const sorted = applySort(filtered, config.resolveField, input.sortBy);

  const limit = input.limit;
  const currentPage = input.page ?? 1;
  const totalItems = sorted.length;
  const isPaginated = limit > 0;
  const offset = isPaginated ? (currentPage - 1) * limit : 0;
  const totalPages = isPaginated ? Math.max(1, Math.ceil(totalItems / limit)) : 1;
  const paginatedData = isPaginated ? sorted.slice(offset, offset + limit) : sorted;

  return {
    data: paginatedData,
    pagination: {
      itemsPerPage: limit,
      totalItems,
      currentPage,
      totalPages,
      sortBy: input.sortBy,
      filter: input.filters,
    },
  };
}
