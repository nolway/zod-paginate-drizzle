import {
  and as drizzleAnd,
  arrayContains as drizzleArrayContains,
  asc as drizzleAsc,
  desc as drizzleDesc,
  eq as drizzleEq,
  gt as drizzleGt,
  gte as drizzleGte,
  ilike as drizzleIlike,
  inArray as drizzleInArray,
  isNull as drizzleIsNull,
  like as drizzleLike,
  lt as drizzleLt,
  lte as drizzleLte,
  not as drizzleNot,
  or as drizzleOr,
} from 'drizzle-orm';
import type { SQL } from 'drizzle-orm';
import type {
  AllowedPath,
  DataSchema,
  Condition,
  PaginationQueryParams,
  SortDirection,
  WhereNode,
} from 'zod-paginate';

export type DrizzleSelectShape<TColumn> = Record<string, TColumn>;

export interface DrizzleOperatorSet<TColumn, TWhereExpr, TOrderByExpr> {
  eq: (column: TColumn, value: unknown) => TWhereExpr;
  isNull: (column: TColumn) => TWhereExpr;
  inArray: (column: TColumn, values: readonly unknown[]) => TWhereExpr;
  gt: (column: TColumn, value: unknown) => TWhereExpr;
  gte: (column: TColumn, value: unknown) => TWhereExpr;
  lt: (column: TColumn, value: unknown) => TWhereExpr;
  lte: (column: TColumn, value: unknown) => TWhereExpr;
  ilike: (column: TColumn, value: string) => TWhereExpr;
  and: (...expressions: TWhereExpr[]) => TWhereExpr;
  or: (...expressions: TWhereExpr[]) => TWhereExpr;
  not: (expression: TWhereExpr) => TWhereExpr;
  asc: (column: TColumn) => TOrderByExpr;
  desc: (column: TColumn) => TOrderByExpr;
  contains?: (column: TColumn, values: readonly string[]) => TWhereExpr;
}

export type DrizzleFieldMap<TSchema extends DataSchema, TColumn> = Partial<
  Record<string, TColumn>
> &
  Partial<Record<AllowedPath<TSchema>, TColumn>>;

export interface BuildDrizzleClausesConfig<
  TSchema extends DataSchema,
  TColumn,
  TWhereExpr,
  TOrderByExpr,
> {
  fields: DrizzleFieldMap<TSchema, TColumn>;
  operators: DrizzleOperatorSet<TColumn, TWhereExpr, TOrderByExpr>;
  selectAlias?: (fieldPath: string) => string;
  strictFieldMapping?: boolean;
}

export interface DrizzlePaginationClauses<TColumn, TWhereExpr, TOrderByExpr> {
  select: DrizzleSelectShape<TColumn>;
  where?: TWhereExpr;
  orderBy?: TOrderByExpr[];
  limit?: number;
  offset?: number;
}

export type DrizzleSqlColumn = Parameters<typeof drizzleIlike>[0];
export type DrizzleSqlOperatorSet = DrizzleOperatorSet<DrizzleSqlColumn, SQL, SQL>;

export type DrizzleDialect = 'pg' | 'mysql';

export interface DrizzleDynamicQuery extends PromiseLike<Record<string, unknown>[]> {
  where(expression: SQL): this;
  orderBy(...expressions: SQL[]): this;
  limit(value: number): this;
  offset(value: number): this;
}

export interface DrizzleAutoQuery extends PromiseLike<Record<string, unknown>[]> {
  $dynamic(): DrizzleDynamicQuery;
}

export interface ApplyDrizzlePaginationOnQueryConfig<
  TSchema extends DataSchema,
  TColumn extends DrizzleSqlColumn,
> {
  dialect: DrizzleDialect;
  buildQuery: (selectShape: DrizzleSelectShape<TColumn>) => DrizzleAutoQuery;
  fields: DrizzleFieldMap<TSchema, TColumn>;
  strictFieldMapping?: boolean;
  selectAlias?: (fieldPath: string) => string;
  operators?: DrizzleSqlOperatorSet;
}

/**
 * Builds a Drizzle `AND` SQL expression and ensures the resulting expression exists.
 */
function andSql(...expressions: SQL[]): SQL {
  const result = drizzleAnd(...expressions);
  if (!result) throw new Error('Cannot build AND expression from an empty list');
  return result;
}

/**
 * Builds a Drizzle `OR` SQL expression and ensures the resulting expression exists.
 */
function orSql(...expressions: SQL[]): SQL {
  const result = drizzleOr(...expressions);
  if (!result) throw new Error('Cannot build OR expression from an empty list');
  return result;
}

/**
 * Ready-to-use operator set for Drizzle + PostgreSQL.
 * Includes `$contains` support via `arrayContains`.
 */
export function createPgDrizzleOperators(): DrizzleSqlOperatorSet {
  return {
    eq: drizzleEq,
    isNull: drizzleIsNull,
    inArray: drizzleInArray,
    gt: drizzleGt,
    gte: drizzleGte,
    lt: drizzleLt,
    lte: drizzleLte,
    ilike: drizzleIlike,
    and: andSql,
    or: orSql,
    not: drizzleNot,
    asc: drizzleAsc,
    desc: drizzleDesc,
    contains: drizzleArrayContains,
  };
}

/**
 * Ready-to-use operator set for Drizzle + MySQL.
 * `$ilike` and `$sw` are mapped to `like` (case-insensitive behavior depends on collation).
 * `$contains` is intentionally not provided by default.
 */
export function createMySqlDrizzleOperators(): DrizzleSqlOperatorSet {
  return {
    eq: drizzleEq,
    isNull: drizzleIsNull,
    inArray: drizzleInArray,
    gt: drizzleGt,
    gte: drizzleGte,
    lt: drizzleLt,
    lte: drizzleLte,
    ilike: drizzleLike,
    and: andSql,
    or: orSql,
    not: drizzleNot,
    asc: drizzleAsc,
    desc: drizzleDesc,
  };
}

/**
 * Returns the default alias for a selected field path.
 */
function defaultSelectAlias(path: string): string {
  return path.replaceAll('.', '_');
}

/**
 * Escapes SQL `LIKE` wildcard characters for literal search values.
 */
function escapeLike(value: string): string {
  return value.replaceAll('\\', '\\\\').replaceAll('%', '\\%').replaceAll('_', '\\_');
}

/**
 * Resolves a mapped Drizzle column from a field path.
 * Throws when strict mapping is enabled and no mapping exists.
 */
function getMappedColumn<TSchema extends DataSchema, TColumn>(
  fieldPath: string,
  fields: DrizzleFieldMap<TSchema, TColumn>,
  strictFieldMapping: boolean,
): TColumn | undefined {
  const mappedColumn = fields[fieldPath];
  if (mappedColumn !== undefined) return mappedColumn;

  if (strictFieldMapping) {
    throw new Error(`No Drizzle field mapping found for "${fieldPath}"`);
  }

  return undefined;
}

/**
 * Builds the Drizzle select shape from requested field paths.
 * Handles alias collisions by suffixing aliases with an index.
 */
function buildSelectShapeInternal<TSchema extends DataSchema, TColumn>(
  selectedFields: readonly string[],
  fields: DrizzleFieldMap<TSchema, TColumn>,
  strictFieldMapping: boolean,
  selectAlias: (fieldPath: string) => string,
): DrizzleSelectShape<TColumn> {
  const selectShape: DrizzleSelectShape<TColumn> = {};
  const aliasUsageCount = new Map<string, number>();

  for (const fieldPath of selectedFields) {
    const mappedColumn = getMappedColumn(fieldPath, fields, strictFieldMapping);
    if (!mappedColumn) continue;

    const baseAlias = selectAlias(fieldPath);
    const collisionIndex = aliasUsageCount.get(baseAlias) ?? 0;
    aliasUsageCount.set(baseAlias, collisionIndex + 1);

    const finalAlias = collisionIndex === 0 ? baseAlias : `${baseAlias}_${collisionIndex}`;
    selectShape[finalAlias] = mappedColumn;
  }

  return selectShape;
}

/**
 * Converts a validated pagination condition into a Drizzle where expression.
 */
function conditionToDrizzleExpr<TColumn, TWhereExpr, TOrderByExpr>(
  condition: Condition,
  column: TColumn,
  operators: DrizzleOperatorSet<TColumn, TWhereExpr, TOrderByExpr>,
): TWhereExpr {
  let expression: TWhereExpr;

  switch (condition.op) {
    case '$null': {
      expression = operators.isNull(column);
      break;
    }
    case '$eq': {
      expression = operators.eq(column, condition.value);
      break;
    }
    case '$in': {
      expression = operators.inArray(column, condition.value);
      break;
    }
    case '$contains': {
      if (!operators.contains) {
        throw new Error(
          'Operator "$contains" is present but no "contains" function is provided in the Drizzle operator set',
        );
      }
      expression = operators.contains(column, condition.value);
      break;
    }
    case '$gt': {
      expression = operators.gt(column, condition.value);
      break;
    }
    case '$gte': {
      expression = operators.gte(column, condition.value);
      break;
    }
    case '$lt': {
      expression = operators.lt(column, condition.value);
      break;
    }
    case '$lte': {
      expression = operators.lte(column, condition.value);
      break;
    }
    case '$btw': {
      const [startValue, endValue] = condition.value;
      expression = operators.and(
        operators.gte(column, startValue),
        operators.lte(column, endValue),
      );
      break;
    }
    case '$ilike': {
      expression = operators.ilike(column, `%${escapeLike(condition.value)}%`);
      break;
    }
    case '$sw': {
      expression = operators.ilike(column, `${escapeLike(condition.value)}%`);
      break;
    }
    default: {
      const unsupportedCondition: never = condition;
      throw new Error(`Unsupported operator in condition: ${JSON.stringify(unsupportedCondition)}`);
    }
  }

  if (condition.not) return operators.not(expression);
  return expression;
}

/**
 * Recursively converts a validated where tree into Drizzle expressions.
 */
function whereNodeToDrizzleExpr<TSchema extends DataSchema, TColumn, TWhereExpr, TOrderByExpr>(
  node: WhereNode,
  fields: DrizzleFieldMap<TSchema, TColumn>,
  operators: DrizzleOperatorSet<TColumn, TWhereExpr, TOrderByExpr>,
  strictFieldMapping: boolean,
): TWhereExpr | undefined {
  if (node.type === 'filter') {
    const mappedColumn = getMappedColumn(node.field, fields, strictFieldMapping);
    if (!mappedColumn) return undefined;
    return conditionToDrizzleExpr(node.condition, mappedColumn, operators);
  }

  const childExpressions = node.items
    .map((itemNode) => whereNodeToDrizzleExpr(itemNode, fields, operators, strictFieldMapping))
    .filter((expression): expression is TWhereExpr => Boolean(expression));

  if (childExpressions.length === 0) return undefined;
  if (childExpressions.length === 1) return childExpressions[0];

  if (node.type === 'and') return operators.and(...childExpressions);
  return operators.or(...childExpressions);
}

/**
 * Maps a pagination sort direction to its corresponding Drizzle order expression.
 */
function directionToOrderExpr<TColumn, TOrderByExpr>(
  direction: SortDirection,
  column: TColumn,
  operators: Pick<DrizzleOperatorSet<TColumn, unknown, TOrderByExpr>, 'asc' | 'desc'>,
): TOrderByExpr {
  return direction === 'ASC' ? operators.asc(column) : operators.desc(column);
}

/**
 * Builds Drizzle-ready select, where, order, limit and offset clauses from parsed pagination.
 */
function buildDrizzleClausesFromPagination<
  TSchema extends DataSchema,
  TColumn,
  TWhereExpr,
  TOrderByExpr,
>(
  parsed: PaginationQueryParams<TSchema>,
  config: BuildDrizzleClausesConfig<TSchema, TColumn, TWhereExpr, TOrderByExpr>,
): DrizzlePaginationClauses<TColumn, TWhereExpr, TOrderByExpr> {
  const strictFieldMapping = config.strictFieldMapping ?? true;
  const aliasBuilder = config.selectAlias ?? defaultSelectAlias;

  const pagination = parsed.pagination;

  const select = buildSelectShapeInternal(
    pagination.select ? pagination.select.map((fieldPath) => `${fieldPath}`) : [],
    config.fields,
    strictFieldMapping,
    aliasBuilder,
  );

  const where = pagination.filters
    ? whereNodeToDrizzleExpr(
        pagination.filters,
        config.fields,
        config.operators,
        strictFieldMapping,
      )
    : undefined;

  const orderBy = pagination.sortBy
    ?.map((sortItem) => {
      const mappedColumn = getMappedColumn(
        `${sortItem.property}`,
        config.fields,
        strictFieldMapping,
      );
      if (!mappedColumn) return undefined;
      return directionToOrderExpr(sortItem.direction, mappedColumn, config.operators);
    })
    .filter((orderExpression): orderExpression is TOrderByExpr => Boolean(orderExpression));

  const limit = pagination.limit;

  let offset: number | undefined;
  if (
    pagination.type === 'LIMIT_OFFSET' &&
    typeof pagination.page === 'number' &&
    limit !== undefined
  ) {
    const safePage = pagination.page > 0 ? pagination.page : 1;
    offset = (safePage - 1) * limit;
  }

  return {
    select,
    where,
    orderBy: orderBy && orderBy.length > 0 ? orderBy : undefined,
    limit,
    offset,
  };
}

/**
 * Returns the default Drizzle operator set for the selected SQL dialect.
 */
function getOperatorsForDialect(dialect: DrizzleDialect): DrizzleSqlOperatorSet {
  if (dialect === 'pg') return createPgDrizzleOperators();
  return createMySqlDrizzleOperators();
}

/**
 * Applies generated Drizzle pagination clauses to an existing Drizzle query builder.
 */
export function applyDrizzlePaginationOnQuery<
  TSchema extends DataSchema,
  TColumn extends DrizzleSqlColumn,
>(
  parsed: PaginationQueryParams<TSchema>,
  config: ApplyDrizzlePaginationOnQueryConfig<TSchema, TColumn>,
): {
  query: DrizzleDynamicQuery;
  clauses: DrizzlePaginationClauses<TColumn, SQL, SQL>;
} {
  const operators = config.operators ?? getOperatorsForDialect(config.dialect);

  const clauses = buildDrizzleClausesFromPagination<TSchema, TColumn, SQL, SQL>(parsed, {
    fields: config.fields,
    operators,
    selectAlias: config.selectAlias,
    strictFieldMapping: config.strictFieldMapping,
  });

  let query = config.buildQuery(clauses.select).$dynamic();

  if (clauses.where) {
    query = query.where(clauses.where);
  }

  if (clauses.orderBy && clauses.orderBy.length > 0) {
    query = query.orderBy(...clauses.orderBy);
  }

  if (typeof clauses.limit === 'number') {
    query = query.limit(clauses.limit);
  }

  if (typeof clauses.offset === 'number') {
    query = query.offset(clauses.offset);
  }

  return { query, clauses };
}
