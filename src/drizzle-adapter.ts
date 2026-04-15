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
  sql,
} from 'drizzle-orm';
import type { Column, GetColumnData, SQL } from 'drizzle-orm';
import type {
  AllowedPath,
  AllowedSelectablePath,
  Condition,
  CursorPaginationResponseMeta,
  DataSchema,
  LimitOffsetPaginationResponseMeta,
  PaginationPayload,
  PaginationType,
  SelectQueryPayload,
  SelectResponse,
  SelectResponseType,
  SortDirection,
  WhereNode,
} from 'zod-paginate';

// ─── Relation types ─────────────────────────────────────────────────────────

/**
 * Controls whether an assembled relation produces an **array** (`'many'`)
 * or a **single object \| null** (`'one'`).
 */
export type RelationMode = 'many' | 'one';

/**
 * Describes a one-to-many (or one-to-one) relation that must be fetched
 * as a separate query and assembled back into the parent rows.
 *
 * @example
 * ```ts
 * // Simple (single FK):
 * const postsRelation: DrizzleRelation<typeof DrizzleSqlColumn> = {
 *   relationName: 'posts',
 *   fields: { id: postsTable.id, title: postsTable.title },
 *   foreignKey: postsTable.authorId,
 *   parentKey: usersTable.id,
 *   buildQuery: (select) => db.select(select).from(postsTable),
 * };
 *
 * // Composite FK:
 * const itemsRelation: DrizzleRelation<typeof DrizzleSqlColumn> = {
 *   relationName: 'items',
 *   fields: { quantity: orderItemsTable.quantity },
 *   foreignKey: [orderItemsTable.orderId, orderItemsTable.productId],
 *   parentKey: [ordersTable.orderId, ordersTable.productId],
 *   buildQuery: (select) => db.select(select).from(orderItemsTable),
 * };
 * ```
 */
export interface DrizzleRelation<
  TColumn,
  TName extends string = string,
  TRelFields extends Record<string, TColumn> = Record<string, TColumn>,
  TMode extends RelationMode = 'many',
> {
  /** Name used as the key in the assembled result (e.g. `"posts"`). */
  relationName: TName;
  /** Column map for the child table — keys are the sub-field names. */
  fields: TRelFields;
  /**
   * The column(s) on the **child** table that reference the parent.
   * Use an array for composite foreign keys.
   */
  foreignKey: TColumn | TColumn[];
  /**
   * The column(s) on the **parent** table referenced by `foreignKey`.
   * Must have the same length as `foreignKey` when using arrays.
   */
  parentKey: TColumn | TColumn[];
  /**
   * Controls how the assembled result is shaped:
   * - `'many'` (default): attaches an **array** of child rows.
   * - `'one'`: attaches a **single object or `null`** (first match).
   */
  mode?: TMode;
  /**
   * Static ordering applied to the relation query.
   *
   * When the client also requests sorting via query params (e.g.
   * `sortBy=posts.createdAt`), the client sort takes priority and
   * this static order acts as a tiebreaker.
   *
   * @example
   * ```ts
   * orderBy: [desc(posts.createdAt)]
   * ```
   */
  orderBy?: SQL[];
  /**
   * Maximum number of child rows **per parent** to keep after assembly.
   *
   * All matching children are still fetched from the database; the
   * limit is applied in-memory during `assembleDrizzleRelations`.
   * Combined with `orderBy`, this lets you express "last N items".
   *
   * Ignored when `mode` is `'one'` (already capped at 1).
   *
   * @example
   * ```ts
   * orderBy: [desc(posts.createdAt)],
   * limit: 5,   // keep the 5 most recent posts per user
   * ```
   */
  limit?: number;
  /**
   * Factory that builds the base query for this relation.
   * Receives the computed select shape (column subset) and must return
   * a Drizzle auto-query (same contract as the main `buildQuery`).
   *
   * Uses method syntax to enable TypeScript bivariance — this allows
   * implementations to use their dialect-specific column types
   * (e.g. `PgColumn`, `MySqlColumn`) without type conflicts.
   */
  buildQuery(selectShape: DrizzleSelectShape<TColumn>): DrizzleAutoQuery;
}

// ─── Relation type inference utilities ────────────────────────────────────

/**
 * Extracts the `relationName` string literal from a `DrizzleRelation`.
 */
export type InferRelationName<TRel> = TRel extends { relationName: infer TName extends string }
  ? TName
  : string;

/**
 * Extracts the `fields` record type from a `DrizzleRelation` and applies
 * `InferFieldsData` to produce the child-row data type.
 */
export type InferRelationRow<TRel> = TRel extends {
  fields: infer TRelFields extends Record<string, unknown>;
}
  ? InferFieldsData<TRelFields>
  : Record<string, unknown>;

/**
 * Structural constraint for `DrizzleRelation` that is as strongly typed as
 * `DrizzleRelation<DrizzleSqlColumn>` for all properties **except** `buildQuery`.
 *
 * `buildQuery` uses `DrizzleSelectShape<DrizzleSqlColumn>` as its parameter
 * type — with method-signature bivariance this is compatible with *any*
 * concrete `DrizzleSelectShape<T extends DrizzleSqlColumn>`, which lets
 * users write `(select) => db.select(select).from(table)` without an
 * explicit type annotation on `select`.
 */
export interface AnyDrizzleRelation {
  /** Name used as the key in the assembled result (e.g. `"posts"`). */
  relationName: string;
  /** Column map for the child table — keys are the sub-field names. */
  fields: Record<string, DrizzleSqlColumn>;
  /**
   * The column(s) on the **child** table that reference the parent.
   * Use an array for composite foreign keys.
   */
  foreignKey: DrizzleSqlColumn | DrizzleSqlColumn[];
  /**
   * The column(s) on the **parent** table referenced by `foreignKey`.
   * Must have the same length as `foreignKey` when using arrays.
   */
  parentKey: DrizzleSqlColumn | DrizzleSqlColumn[];
  /**
   * Controls how the assembled result is shaped:
   * - `'many'` (default): attaches an **array** of child rows.
   * - `'one'`: attaches a **single object or `null`** (first match).
   */
  mode?: RelationMode;
  /**
   * Static ordering applied to the relation query.
   * Acts as a tiebreaker when the client also requests sorting.
   */
  orderBy?: SQL[];
  /**
   * Maximum number of child rows **per parent** to keep after assembly.
   * Ignored when `mode` is `'one'`.
   */
  limit?: number;
  /**
   * Factory that builds the base query for this relation.
   * Receives the computed select shape and must return a Drizzle auto-query.
   */
  buildQuery(selectShape: DrizzleSelectShape<DrizzleSqlColumn>): DrizzleAutoQuery;
}

/**
 * Helper that captures the column type from a relation object literal so
 * that `buildQuery`'s `select` parameter is correctly inferred without
 * manual type annotations.
 *
 * `TFieldColumn` is inferred from `fields` only (covariant position), then
 * used to contextually type the `buildQuery` callback parameter as
 * `Record<string, TFieldColumn>`. `foreignKey` and `parentKey` accept any
 * `DrizzleSqlColumn` independently, avoiding column-type conflicts between
 * child and parent tables.
 *
 * @example
 * ```ts
 * relations: [
 *   defineRelation({
 *     relationName: 'posts',
 *     fields: { id: posts.id, title: posts.title },
 *     foreignKey: posts.authorId,
 *     parentKey: users.id,
 *     buildQuery: (select) => db.select(select).from(posts),
 *   }),
 * ]
 * ```
 */
export function defineRelation<
  TFieldColumn extends DrizzleSqlColumn,
  const TName extends string,
  TRelFields extends Record<string, TFieldColumn>,
>(relation: {
  relationName: TName;
  fields: TRelFields & Record<string, TFieldColumn>;
  foreignKey: DrizzleSqlColumn | DrizzleSqlColumn[];
  parentKey: DrizzleSqlColumn | DrizzleSqlColumn[];
  mode: 'one';
  orderBy?: SQL[];
  limit?: number;
  buildQuery: (selectShape: DrizzleSelectShape<NoInfer<TFieldColumn>>) => DrizzleAutoQuery;
}): AnyDrizzleRelation & { relationName: TName; fields: TRelFields; mode: 'one' };

export function defineRelation<
  TFieldColumn extends DrizzleSqlColumn,
  const TName extends string,
  TRelFields extends Record<string, TFieldColumn>,
>(relation: {
  relationName: TName;
  fields: TRelFields & Record<string, TFieldColumn>;
  foreignKey: DrizzleSqlColumn | DrizzleSqlColumn[];
  parentKey: DrizzleSqlColumn | DrizzleSqlColumn[];
  mode?: 'many';
  orderBy?: SQL[];
  limit?: number;
  buildQuery: (selectShape: DrizzleSelectShape<NoInfer<TFieldColumn>>) => DrizzleAutoQuery;
}): AnyDrizzleRelation & { relationName: TName; fields: TRelFields; mode: 'many' };

export function defineRelation<
  TFieldColumn extends DrizzleSqlColumn,
  const TName extends string,
  TRelFields extends Record<string, TFieldColumn>,
>(relation: {
  relationName: TName;
  fields: TRelFields & Record<string, TFieldColumn>;
  foreignKey: DrizzleSqlColumn | DrizzleSqlColumn[];
  parentKey: DrizzleSqlColumn | DrizzleSqlColumn[];
  mode?: RelationMode;
  orderBy?: SQL[];
  limit?: number;
  buildQuery: (selectShape: DrizzleSelectShape<NoInfer<TFieldColumn>>) => DrizzleAutoQuery;
}): AnyDrizzleRelation & { relationName: TName; fields: TRelFields; mode: RelationMode } {
  // The input is structurally compatible — AnyDrizzleRelation.buildQuery uses
  // method syntax, so TypeScript's parameter bivariance allows assigning a
  // concrete `DrizzleSelectShape<TFieldColumn>` callback to the wider
  // `DrizzleSelectShape<DrizzleSqlColumn>` signature.
  const result: AnyDrizzleRelation & {
    relationName: TName;
    fields: TRelFields;
    mode: RelationMode;
  } = { ...relation, mode: relation.mode ?? 'many' };
  return result;
}

/**
 * Extracts the `mode` from a `DrizzleRelation`.
 * Defaults to `'many'` when not specified.
 */
export type InferRelationMode<TRel> = TRel extends { mode: infer TMode extends RelationMode }
  ? TMode
  : 'many';

/**
 * Given a **tuple** of `DrizzleRelation` entries, builds the intersection
 * type `{ name1: Row1[]; name2: Row2 | null; … }` so the assembled result is
 * fully typed.
 *
 * Relations with `mode: 'one'` produce `Row | null`; all others produce `Row[]`.
 *
 * @example
 * ```ts
 * type Rels = [DrizzleRelation<Col, 'posts', { id: Col; title: Col }>];
 * type Data = InferRelationsData<Rels>;
 * //   ^? { posts: Partial<{ id: number; title: string | null }>[] }
 * ```
 */
export type InferRelationsData<TRelations extends readonly AnyDrizzleRelation[]> =
  TRelations extends readonly [infer TFirst, ...infer TRest]
    ? Record<
        InferRelationName<TFirst>,
        InferRelationMode<TFirst> extends 'one'
          ? InferRelationRow<TFirst> | null
          : InferRelationRow<TFirst>[]
      > &
        (TRest extends readonly AnyDrizzleRelation[] ? InferRelationsData<TRest> : unknown)
    : unknown;

/**
 * The assembled row type: main fields intersected with all relation arrays.
 *
 * @example
 * ```ts
 * type Row = InferAssembledRow<{ id: ColType; name: ColType }, Relations>;
 * //   ^? Partial<{ id: number; name: string | null }> & { posts: …[] }
 * ```
 */
export type InferAssembledRow<
  TFields extends Record<string, unknown>,
  TRelations extends readonly AnyDrizzleRelation[],
> = InferFieldsData<TFields> & InferRelationsData<TRelations>;

/**
 * A single relation query result returned alongside the main query.
 */
export interface DrizzleRelationQuery<TColumn> {
  /** The relation name (matches `DrizzleRelation.relationName`). */
  relationName: string;
  /** The parent-side column(s) to match rows against. */
  parentKey: TColumn | TColumn[];
  /** The alias(es) used for the foreign key(s) in the child select shape. */
  foreignKeyAlias: string | string[];
  /** Assembly mode: `'many'` → array, `'one'` → single object or null. */
  mode: RelationMode;
  /** Max children per parent (applied during assembly). `undefined` = no limit. */
  limit?: number;
  /** The ready-to-execute Drizzle dynamic query. */
  query: DrizzleDynamicQuery;
}

/**
 * Extended result of `generatePaginationQuery`.
 *
 * Provides both low-level access (`query`, `clauses`, `relationQueries`,
 * `assemble`) **and** a high-level `execute()` that runs every query,
 * assembles relations and computes pagination metadata in a single call.
 */
export interface DrizzlePaginationResult<
  TColumn,
  TFields extends Record<string, unknown> = Record<string, unknown>,
  TRelations extends readonly AnyDrizzleRelation[] = readonly AnyDrizzleRelation[],
  TType extends PaginationType = PaginationType,
> {
  /** Ready-to-execute main query (with WHERE / ORDER / LIMIT / OFFSET applied). */
  query: DrizzleDynamicQuery;
  /** Raw pagination clauses for advanced use-cases. */
  clauses: DrizzlePaginationClauses<TColumn, SQL, SQL>;
  /** Per-relation sub-queries. */
  relationQueries: DrizzleRelationQuery<DrizzleSqlColumn>[];
  /**
   * Low-level helper: assembles main rows + relation result arrays into
   * nested objects. Same logic as the standalone `assembleDrizzleRelations`.
   */
  assemble: (
    mainRows: Record<string, unknown>[],
    relationResults: Record<string, unknown>[][],
  ) => InferAssembledRow<TFields, TRelations>[];
  /**
   * Executes **all** queries (main + count + relations), assembles nested
   * objects and builds the pagination metadata in one call.
   *
   * @returns `{ data, pagination }` — ready to send as the HTTP response body.
   */
  execute: () => Promise<DrizzlePaginationExecuteResult<TFields, TRelations, TType>>;
}

/** Maps a `PaginationType` to its corresponding response metadata type. */
export type InferPaginationResponseMeta<TType extends PaginationType = PaginationType> =
  TType extends 'LIMIT_OFFSET'
    ? LimitOffsetPaginationResponseMeta
    : TType extends 'CURSOR'
      ? CursorPaginationResponseMeta
      : LimitOffsetPaginationResponseMeta | CursorPaginationResponseMeta;

/** Return type of `DrizzlePaginationResult.execute()`. */
export interface DrizzlePaginationExecuteResult<
  TFields extends Record<string, unknown> = Record<string, unknown>,
  TRelations extends readonly AnyDrizzleRelation[] = readonly AnyDrizzleRelation[],
  TType extends PaginationType = PaginationType,
> {
  data: InferAssembledRow<TFields, TRelations>[];
  pagination: InferPaginationResponseMeta<TType>;
}

/** Resolves the `data` type based on `SelectResponseType`. */
export type InferSelectExecuteData<
  TFields extends Record<string, unknown>,
  TRelations extends readonly AnyDrizzleRelation[],
  TResponseType extends SelectResponseType,
> = TResponseType extends 'one'
  ? InferAssembledRow<TFields, TRelations> | null
  : InferAssembledRow<TFields, TRelations>[];

/**
 * Result of `generateSelectQuery`.
 *
 * Lighter variant of `DrizzlePaginationResult` — no pagination
 * clauses. Includes `assemble` and `execute` helpers.
 */
export interface DrizzleSelectWithRelationsResult<
  TSchema extends DataSchema = DataSchema,
  TFields extends Record<string, unknown> = Record<string, unknown>,
  TRelations extends readonly AnyDrizzleRelation[] = readonly AnyDrizzleRelation[],
  TResponseType extends SelectResponseType = 'many',
> {
  /** Ready-to-execute main query. */
  query: DrizzleDynamicQuery;
  /** Per-relation sub-queries. */
  relationQueries: DrizzleRelationQuery<DrizzleSqlColumn>[];
  /**
   * Low-level helper: assembles main rows + relation result arrays into
   * nested objects.
   */
  assemble: (
    mainRows: Record<string, unknown>[],
    relationResults: Record<string, unknown>[][],
  ) => InferAssembledRow<TFields, TRelations>[];
  /**
   * Executes **all** queries (main + relations), assembles nested objects
   * and returns `{ data }` (or `null` when `responseType` is `'one'` and no
   * row is found).
   */
  execute: () => Promise<
    TResponseType extends 'one'
      ? SelectResponse<TSchema, AllowedSelectablePath<TSchema>, 'one'> | null
      : SelectResponse<TSchema, AllowedSelectablePath<TSchema>, TResponseType>
  >;
}

export type DrizzleSelectShape<TColumn> = Record<string, TColumn>;

/**
 * Infers query-mode data types from a Drizzle fields map.
 * Handles nullable columns correctly via `GetColumnData` (adds `| null` when
 * the column is not marked as `notNull`).
 *
 * Result is a `Partial` because pagination may select only a subset of fields.
 */
export type InferFieldsData<TFields extends Record<string, unknown>> = Partial<{
  [K in keyof TFields]: TFields[K] extends Column ? GetColumnData<TFields[K]> : unknown;
}>;

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
  /** Present only for `CURSOR` pagination — the incoming cursor value (if any). */
  cursor?: number | string;
  /** Present only for `CURSOR` pagination — the resolved cursor property name. */
  cursorProperty?: string;
}

export type DrizzleSqlColumn = Parameters<typeof drizzleIlike>[0];
export type DrizzleSqlOperatorSet = DrizzleOperatorSet<DrizzleSqlColumn, SQL, SQL>;

export type DrizzleDialect = 'pg' | 'mysql';

export interface DrizzleDynamicQuery<
  TResult = Record<string, unknown>[],
> extends PromiseLike<TResult> {
  where(expression: SQL): DrizzleDynamicQuery<TResult>;
  orderBy(...expressions: SQL[]): DrizzleDynamicQuery<TResult>;
  limit(value: number): DrizzleDynamicQuery<TResult>;
  offset(value: number): DrizzleDynamicQuery<TResult>;
}

export interface DrizzleAutoQuery<
  TResult = Record<string, unknown>[],
> extends PromiseLike<TResult> {
  $dynamic(): DrizzleDynamicQuery<TResult>;
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
 *
 * An optional `overrides` parameter allows callers to substitute select, filters
 * and sortBy without reconstructing the `PaginationPayload` object (which is a
 * conditional type that cannot be spread safely).
 */
function buildDrizzleClausesFromPagination<
  TSchema extends DataSchema,
  TColumn,
  TWhereExpr,
  TOrderByExpr,
>(
  pagination: PaginationPayload<TSchema>,
  config: BuildDrizzleClausesConfig<TSchema, TColumn, TWhereExpr, TOrderByExpr>,
  overrides?: {
    select?: readonly string[];
    filters?: WhereNode;
    sortBy?: readonly { property: string; direction: SortDirection }[];
  },
): DrizzlePaginationClauses<TColumn, TWhereExpr, TOrderByExpr> {
  const strictFieldMapping = config.strictFieldMapping ?? true;
  const aliasBuilder = config.selectAlias ?? defaultSelectAlias;

  const effectiveSelect = overrides && 'select' in overrides ? overrides.select : pagination.select;
  const effectiveFilters =
    overrides && 'filters' in overrides ? overrides.filters : pagination.filters;
  const effectiveSortBy = overrides && 'sortBy' in overrides ? overrides.sortBy : pagination.sortBy;

  const select = buildSelectShapeInternal(
    effectiveSelect ? effectiveSelect.map((fieldPath) => fieldPath) : [],
    config.fields,
    strictFieldMapping,
    aliasBuilder,
  );

  let where = effectiveFilters
    ? whereNodeToDrizzleExpr(effectiveFilters, config.fields, config.operators, strictFieldMapping)
    : undefined;

  const orderBy = effectiveSortBy
    ?.map((sortItem) => {
      const mappedColumn = getMappedColumn(sortItem.property, config.fields, strictFieldMapping);
      if (!mappedColumn) return undefined;
      return directionToOrderExpr(sortItem.direction, mappedColumn, config.operators);
    })
    .filter((orderExpression): orderExpression is TOrderByExpr => Boolean(orderExpression));

  const limit = pagination.limit;

  let offset: number | undefined;
  let cursor: number | string | undefined;
  let cursorProperty: string | undefined;

  if (pagination.type === 'LIMIT_OFFSET' && typeof pagination.page === 'number') {
    const safePage = pagination.page > 0 ? pagination.page : 1;
    offset = (safePage - 1) * limit;
  }

  // ── Cursor-based pagination ───────────────────────────────────
  if (pagination.type === 'CURSOR') {
    cursorProperty = `${pagination.cursorProperty}`;

    if (pagination.cursor !== undefined) {
      cursor = pagination.cursor;
      const cursorColumn = getMappedColumn(cursorProperty, config.fields, strictFieldMapping);

      if (cursorColumn) {
        // Determine direction from sortBy: if the cursor property has DESC → use "<", else ">".
        const cursorSort = effectiveSortBy?.find((s) => s.property === cursorProperty);
        const cursorDirection = cursorSort?.direction ?? 'ASC';
        const cursorExpr =
          cursorDirection === 'DESC'
            ? config.operators.lt(cursorColumn, cursor)
            : config.operators.gt(cursorColumn, cursor);

        // Combine with existing where clause.
        where = where ? config.operators.and(where, cursorExpr) : cursorExpr;
      }
    }
  }

  return {
    select,
    where,
    orderBy: orderBy && orderBy.length > 0 ? orderBy : undefined,
    limit,
    offset,
    cursor,
    cursorProperty,
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
  TFields extends Record<string, TColumn>,
>(
  parsed: PaginationPayload<TSchema>,
  config: {
    dialect: DrizzleDialect;
    buildQuery: (
      selectShape: DrizzleSelectShape<NoInfer<TColumn>>,
    ) => DrizzleAutoQuery<InferFieldsData<NoInfer<TFields>>[]>;
    fields: TFields & DrizzleFieldMap<TSchema, TColumn>;
    strictFieldMapping?: boolean;
    selectAlias?: (fieldPath: string) => string;
    operators?: DrizzleSqlOperatorSet;
  },
): {
  query: DrizzleDynamicQuery<InferFieldsData<TFields>[]>;
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

// ─── Relation support ─────────────────────────────────────────────────────

/** Internal base for the foreign key alias injected into relation selects. */
const RELATION_FK_ALIAS = '__fk';

/** Wraps a single value or array into a consistent array. */
function toArray<T>(value: T | T[]): T[] {
  return Array.isArray(value) ? value : [value];
}

/**
 * Builds the foreign key alias(es) for a relation.
 * Single FK → `'__fk'`, composite → `['__fk_0', '__fk_1', ...]`.
 */
function buildFkAliases(foreignKeys: unknown[]): string | string[] {
  if (foreignKeys.length === 1) return RELATION_FK_ALIAS;
  const aliases: string[] = [];
  for (let i = 0; i < foreignKeys.length; i++) {
    aliases.push(`${RELATION_FK_ALIAS}_${i}`);
  }
  return aliases;
}

/**
 * Builds the parent key alias(es) for a relation.
 * Single PK → `'__pk_name'`, composite → `['__pk_name_0', '__pk_name_1', ...]`.
 */
function buildPkAliases(relationName: string, parentKeys: unknown[]): string | string[] {
  if (parentKeys.length === 1) return `__pk_${relationName}`;
  const aliases: string[] = [];
  for (let i = 0; i < parentKeys.length; i++) {
    aliases.push(`__pk_${relationName}_${i}`);
  }
  return aliases;
}

/**
 * Builds a composite lookup key from multiple values.
 * Single value → used directly, composite → JSON-serialized tuple.
 */
function compositeKey(aliases: string | string[], row: Record<string, unknown>): unknown {
  if (typeof aliases === 'string') return row[aliases];
  const parts = aliases.map((a) => row[a]);
  return JSON.stringify(parts);
}

/** Returns true when a composite key contains no null/undefined parts. */
function isKeyValid(aliases: string | string[], row: Record<string, unknown>): boolean {
  if (typeof aliases === 'string') {
    const v = row[aliases];
    return v !== undefined && v !== null;
  }
  return aliases.every((a) => {
    const v = row[a];
    return v !== undefined && v !== null;
  });
}

/** Returns a Set of all internal alias strings (handles both single and composite). */
function collectAliases(aliases: string | string[]): string[] {
  return typeof aliases === 'string' ? [aliases] : aliases;
}

/**
 * Checks whether a field path belongs to a relation (starts with `"relationName."`).
 * If so, returns the sub-path (e.g. `"posts.title"` → `"title"`).
 */
function stripRelationPrefix(fieldPath: string, relationName: string): string | undefined {
  const prefix = `${relationName}.`;
  if (fieldPath.startsWith(prefix)) {
    return fieldPath.slice(prefix.length);
  }
  return undefined;
}

/**
 * Returns `true` when any relation name matches the field path prefix.
 */
function belongsToAnyRelation(fieldPath: string, relationNames: string[]): boolean {
  return relationNames.some((name) => stripRelationPrefix(fieldPath, name) !== undefined);
}

/**
 * Recursively rewrites a `WhereNode` tree, keeping only filters that belong
 * to a given relation prefix (stripping the prefix from field names).
 * Returns `undefined` when no filter in the subtree matches.
 */
function rewriteWhereNodeForRelation(node: WhereNode, relationName: string): WhereNode | undefined {
  if (node.type === 'filter') {
    const subPath = stripRelationPrefix(node.field, relationName);
    if (subPath === undefined) return undefined;
    return { ...node, field: subPath };
  }

  const rewrittenItems = node.items
    .map((child) => rewriteWhereNodeForRelation(child, relationName))
    .filter((child): child is WhereNode => child !== undefined);

  if (rewrittenItems.length === 0) return undefined;
  if (rewrittenItems.length === 1) return rewrittenItems[0];

  return { ...node, items: rewrittenItems };
}

/**
 * Recursively rewrites a `WhereNode` tree, removing filters that belong
 * to any of the provided relation names (keeping only main-table filters).
 * Returns `undefined` when all filters were stripped.
 */
function rewriteWhereNodeWithoutRelations(
  node: WhereNode,
  relationNames: string[],
): WhereNode | undefined {
  if (node.type === 'filter') {
    return belongsToAnyRelation(node.field, relationNames) ? undefined : node;
  }

  const keptItems = node.items
    .map((child) => rewriteWhereNodeWithoutRelations(child, relationNames))
    .filter((child): child is WhereNode => child !== undefined);

  if (keptItems.length === 0) return undefined;
  if (keptItems.length === 1) return keptItems[0];

  return { ...node, items: keptItems };
}

/**
 * Executes a `COUNT(*)` query using the provided `buildQuery` factory.
 *
 * Works around the type mismatch between `TColumn`-typed `buildQuery` and
 * the `SQL` type of `sql\`count(*)\`` by leveraging the fact that Drizzle's
 * runtime `select()` accepts any column-like value.
 */
async function executeCountQuery<TColumn extends DrizzleSqlColumn>(
  buildQuery: (selectShape: DrizzleSelectShape<TColumn>) => DrizzleAutoQuery,
  where: SQL | undefined,
): Promise<number> {
  const countSelect = { count: sql<number>`count(*)` };
  // @ts-expect-error - Drizzle accepts SQL in select shape at runtime
  let countQuery = buildQuery(countSelect).$dynamic();
  if (where) {
    countQuery = countQuery.where(where);
  }
  const countRows: Record<string, unknown>[] = await countQuery;
  const rawCount = countRows[0]?.count;
  return Number(rawCount ?? 0);
}

/**
 * Builds a SQL filter to scope relation queries to the parent IDs present in
 * the main query results.
 *
 * Single FK  → `foreignKey IN (id1, id2, …)`
 * Composite  → `(fk1 = v1 AND fk2 = v2) OR …`
 *
 * Returns `undefined` when no valid parent IDs are found (the relation query
 * should be skipped entirely in that case).
 */
function buildRelationScope(
  mainRows: Record<string, unknown>[],
  relationName: string,
  parentKey: DrizzleSqlColumn | DrizzleSqlColumn[],
  foreignKey: DrizzleSqlColumn | DrizzleSqlColumn[],
  operators: DrizzleSqlOperatorSet,
): SQL | undefined {
  const parentKeys = toArray(parentKey);
  const foreignKeys = toArray(foreignKey);
  const pkAliases = collectAliases(buildPkAliases(relationName, parentKeys));

  if (foreignKeys.length === 1) {
    const pkAlias = pkAliases[0];
    if (!pkAlias) return undefined;
    const ids = new Set<unknown>();
    for (const row of mainRows) {
      const v = row[pkAlias];
      if (v != null) ids.add(v);
    }
    if (ids.size === 0) return undefined;
    const fk = foreignKeys[0];
    if (!fk) return undefined;
    return operators.inArray(fk, [...ids]);
  }

  // Composite FK: OR of AND(eq(fk_0, v_0), eq(fk_1, v_1), …)
  const seen = new Set<string>();
  const conditions: SQL[] = [];
  for (const row of mainRows) {
    const tuple = pkAliases.map((a) => row[a]);
    if (tuple.some((v) => v == null)) continue;
    const key = JSON.stringify(tuple);
    if (!seen.has(key)) {
      seen.add(key);
      const parts = foreignKeys.map((fk, i) => operators.eq(fk, tuple[i]));
      conditions.push(andSql(...parts));
    }
  }
  if (conditions.length === 0) return undefined;
  if (conditions.length === 1) {
    const single = conditions[0];
    if (!single) return undefined;
    return single;
  }
  return orSql(...conditions);
}

/**
 * Builds a single relation query from the parsed pagination, extracting only
 * the select/filter/sort fields prefixed with the relation name.
 */
function buildSingleRelationQuery(
  pagination: PaginationPayload<DataSchema>,
  relation: AnyDrizzleRelation,
  operators: DrizzleSqlOperatorSet,
  selectAlias: (fieldPath: string) => string,
  strictFieldMapping: boolean,
  parentScope?: SQL,
): DrizzleRelationQuery<DrizzleSqlColumn> {
  // ── Select ──────────────────────────────────────────────────────
  const relationSelectPaths: string[] = [];

  if (pagination.select) {
    for (const fieldPath of pagination.select) {
      const subPath = stripRelationPrefix(fieldPath, relation.relationName);
      if (subPath !== undefined) {
        relationSelectPaths.push(subPath);
      }
    }
  }

  const relationFields: DrizzleFieldMap<DataSchema, DrizzleSqlColumn> = relation.fields;

  const selectShape = buildSelectShapeInternal(
    relationSelectPaths,
    relationFields,
    strictFieldMapping,
    selectAlias,
  );

  // Always include the foreign key(s) so we can match parent ↔ child.
  const foreignKeys = toArray(relation.foreignKey);
  const fkAliases = buildFkAliases(foreignKeys);
  const fkAliasList = collectAliases(fkAliases);
  for (let i = 0; i < foreignKeys.length; i++) {
    const alias = fkAliasList[i];
    const col = foreignKeys[i];
    if (alias !== undefined && col !== undefined) {
      selectShape[alias] = col;
    }
  }

  // ── Filters ─────────────────────────────────────────────────────
  let relationWhere: SQL | undefined;

  if (pagination.filters) {
    const rewrittenFilters = rewriteWhereNodeForRelation(pagination.filters, relation.relationName);
    if (rewrittenFilters) {
      relationWhere = whereNodeToDrizzleExpr(
        rewrittenFilters,
        relationFields,
        operators,
        strictFieldMapping,
      );
    }
  }

  // ── Sort ────────────────────────────────────────────────────────
  const relationOrderBy: SQL[] = [];

  if (pagination.sortBy) {
    for (const sortItem of pagination.sortBy) {
      const subPath = stripRelationPrefix(sortItem.property, relation.relationName);
      if (subPath === undefined) continue;

      const mappedColumn = getMappedColumn(subPath, relationFields, strictFieldMapping);
      if (!mappedColumn) continue;

      relationOrderBy.push(directionToOrderExpr(sortItem.direction, mappedColumn, operators));
    }
  }

  // Append static relation orderBy as fallback / tiebreaker.
  if (relation.orderBy && relation.orderBy.length > 0) {
    relationOrderBy.push(...relation.orderBy);
  }

  // ── Build query ─────────────────────────────────────────────────
  let query = relation.buildQuery(selectShape).$dynamic();

  // Combine relation-level filters with parent-scope IN clause.
  const combinedWhere =
    relationWhere && parentScope
      ? andSql(relationWhere, parentScope)
      : (relationWhere ?? parentScope);

  if (combinedWhere) {
    query = query.where(combinedWhere);
  }
  if (relationOrderBy.length > 0) {
    query = query.orderBy(...relationOrderBy);
  }

  return {
    relationName: relation.relationName,
    parentKey: relation.parentKey,
    foreignKeyAlias: fkAliases,
    mode: relation.mode ?? 'many',
    limit: relation.limit,
    query,
  };
}

/**
 * Builds a select-only relation query (no filters / sort).
 * Used by `generateSelectQuery` and its scoped `execute()`.
 */
function buildSelectOnlyRelationQuery(
  allSelectPaths: readonly string[],
  relation: AnyDrizzleRelation,
  selectAlias: (fieldPath: string) => string,
  strictFieldMapping: boolean,
  parentScope?: SQL,
): DrizzleRelationQuery<DrizzleSqlColumn> {
  const relationSelectPaths: string[] = [];
  for (const fieldPath of allSelectPaths) {
    const subPath = stripRelationPrefix(fieldPath, relation.relationName);
    if (subPath !== undefined) {
      relationSelectPaths.push(subPath);
    }
  }

  const relationFields: DrizzleFieldMap<DataSchema, DrizzleSqlColumn> = relation.fields;

  const selectShape = buildSelectShapeInternal(
    relationSelectPaths,
    relationFields,
    strictFieldMapping,
    selectAlias,
  );

  // Always include the foreign key(s) so we can match parent ↔ child.
  const foreignKeys = toArray(relation.foreignKey);
  const fkAliases = buildFkAliases(foreignKeys);
  const fkAliasList = collectAliases(fkAliases);
  for (let j = 0; j < foreignKeys.length; j++) {
    const alias = fkAliasList[j];
    const col = foreignKeys[j];
    if (alias !== undefined && col !== undefined) {
      selectShape[alias] = col;
    }
  }

  let relationQuery: DrizzleDynamicQuery = relation.buildQuery(selectShape).$dynamic();

  if (parentScope) {
    relationQuery = relationQuery.where(parentScope);
  }

  // Apply static relation orderBy.
  if (relation.orderBy && relation.orderBy.length > 0) {
    relationQuery = relationQuery.orderBy(...relation.orderBy);
  }

  return {
    relationName: relation.relationName,
    parentKey: relation.parentKey,
    foreignKeyAlias: fkAliases,
    mode: relation.mode ?? 'many',
    limit: relation.limit,
    query: relationQuery,
  };
}

/** Configuration object for `generatePaginationQuery`. */
export interface GeneratePaginationQueryConfig<
  TSchema extends DataSchema,
  TColumn extends DrizzleSqlColumn,
  TFields extends Record<string, TColumn>,
  TRelations extends readonly AnyDrizzleRelation[],
> {
  /** Database dialect — determines the default operator set (`'pg'` or `'mysql'`). */
  dialect: DrizzleDialect;
  /**
   * Factory that receives the generated select shape and must return a Drizzle
   * query builder (e.g. `db.select(select).from(table)`).
   */
  buildQuery: (
    selectShape: DrizzleSelectShape<NoInfer<TColumn>>,
  ) => DrizzleAutoQuery<InferFieldsData<NoInfer<TFields>>[]>;
  /**
   * Map from allowed field paths (used in `select`, `filters`, `sortBy`) to
   * Drizzle column references.
   */
  fields: TFields & DrizzleFieldMap<TSchema, TColumn>;
  /** Relations to fetch as separate queries and assemble into parent rows. */
  relations?: TRelations;
  /**
   * When `true` (default), throws if a requested field has no mapping in `fields`.
   * Set to `false` to silently ignore unmapped fields.
   */
  strictFieldMapping?: boolean;
  /**
   * Custom alias generator for select keys. Receives a dotted field path
   * (e.g. `"posts.title"`) and must return a valid SQL alias.
   * Defaults to replacing dots with underscores (`"posts_title"`).
   */
  selectAlias?: (fieldPath: string) => string;
  /**
   * Custom operator set. When omitted, the default operator set for the
   * configured `dialect` is used.
   */
  operators?: DrizzleSqlOperatorSet;
}

/**
 * Applies Drizzle pagination **with relation support**.
 *
 * Works exactly like `applyDrizzlePaginationOnQuery` for the main table,
 * but also builds separate queries for each declared `relation`.
 *
 * Relation-prefixed fields (e.g. `"posts.title"`) in `select`, `filters`, and
 * `sortBy` are **automatically routed** to the corresponding relation query and
 * **stripped from the main query**.
 *
 * After executing all queries, use `assembleDrizzleRelations` to reconstruct
 * nested objects.
 *
 * @example
 * ```ts
 * const result = generatePaginationQuery(parsed, {
 *   dialect: 'pg',
 *   buildQuery: (select) => db.select(select).from(usersTable),
 *   fields: { id: usersTable.id, name: usersTable.name },
 *   relations: [
 *     {
 *       relationName: 'posts',
 *       fields: { id: postsTable.id, title: postsTable.title },
 *       foreignKey: postsTable.authorId,
 *       parentKey: usersTable.id,
 *       buildQuery: (select) => db.select(select).from(postsTable),
 *     },
 *   ],
 * });
 *
 * // One-call approach:
 * const { data, pagination } = await result.execute();
 *
 * // Or manual approach:
 * const [mainRows, ...relationRows] = await Promise.all([
 *   result.query,
 *   ...result.relationQueries.map((r) => r.query),
 * ]);
 * const data = result.assemble(mainRows, relationRows);
 * ```
 */
export function generatePaginationQuery<
  TSchema extends DataSchema,
  TColumn extends DrizzleSqlColumn,
  TFields extends Record<string, TColumn>,
  const TRelations extends readonly AnyDrizzleRelation[],
>(
  parsed: PaginationPayload<TSchema, 'LIMIT_OFFSET'>,
  config: GeneratePaginationQueryConfig<TSchema, TColumn, TFields, TRelations>,
): DrizzlePaginationResult<TColumn, TFields, TRelations, 'LIMIT_OFFSET'>;

export function generatePaginationQuery<
  TSchema extends DataSchema,
  TColumn extends DrizzleSqlColumn,
  TFields extends Record<string, TColumn>,
  const TRelations extends readonly AnyDrizzleRelation[],
>(
  parsed: PaginationPayload<TSchema, 'CURSOR'>,
  config: GeneratePaginationQueryConfig<TSchema, TColumn, TFields, TRelations>,
): DrizzlePaginationResult<TColumn, TFields, TRelations, 'CURSOR'>;

export function generatePaginationQuery<
  TSchema extends DataSchema,
  TColumn extends DrizzleSqlColumn,
  TFields extends Record<string, TColumn>,
  const TRelations extends readonly AnyDrizzleRelation[],
  TType extends PaginationType = PaginationType,
>(
  parsed: PaginationPayload<TSchema, TType>,
  config: GeneratePaginationQueryConfig<TSchema, TColumn, TFields, TRelations>,
): DrizzlePaginationResult<TColumn, TFields, TRelations, TType>;

export function generatePaginationQuery<
  TSchema extends DataSchema,
  TColumn extends DrizzleSqlColumn,
  TFields extends Record<string, TColumn>,
  const TRelations extends readonly AnyDrizzleRelation[],
>(
  parsed: PaginationPayload<TSchema>,
  config: GeneratePaginationQueryConfig<TSchema, TColumn, TFields, TRelations>,
): DrizzlePaginationResult<TColumn, TFields, TRelations> {
  const operators = config.operators ?? getOperatorsForDialect(config.dialect);
  const aliasBuilder = config.selectAlias ?? defaultSelectAlias;
  const strictFieldMapping = config.strictFieldMapping ?? true;
  // @ts-expect-error -- empty array is a valid runtime fallback for TRelations
  const relations: TRelations = config.relations ?? [];
  const relationNames = relations.map((r) => r.relationName);

  // ── Partition the parsed pagination ─────────────────────────────
  const pagination = parsed;

  // Remove relation-prefixed paths from the main select.
  const mainSelect = pagination.select
    ? pagination.select.filter((fp) => !belongsToAnyRelation(`${fp}`, relationNames))
    : undefined;

  // Remove relation-prefixed filters from the main query.
  const mainFilters = pagination.filters
    ? rewriteWhereNodeWithoutRelations(pagination.filters, relationNames)
    : undefined;

  // Remove relation-prefixed sort items from the main query.
  const mainSortBy = pagination.sortBy
    ? pagination.sortBy.filter(
        (sortItem) => !belongsToAnyRelation(`${sortItem.property}`, relationNames),
      )
    : undefined;

  // Inject parent key columns (needed for assembling).
  const parentKeyFields: DrizzleSelectShape<DrizzleSqlColumn> = {};
  for (const relation of relations) {
    const parentKeys = toArray(relation.parentKey);
    const pkAliases = collectAliases(buildPkAliases(relation.relationName, parentKeys));
    for (let i = 0; i < parentKeys.length; i++) {
      const alias = pkAliases[i];
      const col = parentKeys[i];
      if (alias !== undefined && col !== undefined) {
        parentKeyFields[alias] = col;
      }
    }
  }

  // Build the main pagination clauses (without relation fields).
  const clauses = buildDrizzleClausesFromPagination<TSchema, TColumn, SQL, SQL>(
    pagination,
    {
      fields: config.fields,
      operators,
      selectAlias: aliasBuilder,
      strictFieldMapping,
    },
    {
      select: mainSelect,
      filters: mainFilters,
      sortBy: mainSortBy && mainSortBy.length > 0 ? mainSortBy : undefined,
    },
  );

  // Inject parent key columns into the select shape.
  Object.assign(clauses.select, parentKeyFields);

  // Inject the cursor property column so that cursor metadata can always be
  // computed, even when the client did not explicitly select the field.
  if (clauses.cursorProperty) {
    const cursorAlias = aliasBuilder(clauses.cursorProperty);
    if (!(cursorAlias in clauses.select)) {
      const cursorCol = config.fields[clauses.cursorProperty];
      if (cursorCol) {
        clauses.select[cursorAlias] = cursorCol;
      }
    }
  }

  let query: DrizzleDynamicQuery = config.buildQuery(clauses.select).$dynamic();

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

  // ── Build relation queries ──────────────────────────────────────
  const relationQueries = relations.map((relation) =>
    buildSingleRelationQuery(pagination, relation, operators, aliasBuilder, strictFieldMapping),
  );

  // ── assemble / execute helpers ──────────────────────────────────
  type AssembledRow = InferAssembledRow<TFields, TRelations>;

  const assemble = (
    mainRows: Record<string, unknown>[],
    relationResults: Record<string, unknown>[][],
  ): AssembledRow[] =>
    coerceAssembledRows<AssembledRow>(
      assembleDrizzleRelations(mainRows, relationQueries, relationResults),
    );

  type ExecuteResult = DrizzlePaginationExecuteResult<TFields, TRelations>;

  const execute = async (): Promise<ExecuteResult> => {
    // Start count query early so it runs in parallel with the main query.
    const countPromise: PromiseLike<number> =
      parsed.type === 'LIMIT_OFFSET'
        ? executeCountQuery(config.buildQuery, clauses.where)
        : Promise.resolve(0);

    // Execute main query first to obtain parent IDs for relation scoping.
    const mainRows: Record<string, unknown>[] = await query;

    // Build scoped relation queries (filtered by parent IDs).
    const scopedQueries = relations.map((relation) => {
      const scope = buildRelationScope(
        mainRows,
        relation.relationName,
        relation.parentKey,
        relation.foreignKey,
        operators,
      );
      return buildSingleRelationQuery(
        pagination,
        relation,
        operators,
        aliasBuilder,
        strictFieldMapping,
        scope,
      );
    });

    const scopedRelationResults =
      scopedQueries.length > 0 ? await Promise.all(scopedQueries.map((rq) => rq.query)) : [];

    const data = coerceAssembledRows<AssembledRow>(
      assembleDrizzleRelations(mainRows, scopedQueries, scopedRelationResults),
    );

    // Build pagination metadata depending on the type.
    if (parsed.type === 'LIMIT_OFFSET') {
      const totalItems = await countPromise;

      const paginationMeta = buildLimitOffsetResponseMeta(parsed, totalItems);
      return { data, pagination: paginationMeta };
    }

    // CURSOR
    const paginationMeta = buildCursorResponseMeta(parsed, mainRows, undefined, aliasBuilder);
    return { data, pagination: paginationMeta };
  };

  return { query, clauses, relationQueries, assemble, execute };
}

/**
 * Applies Drizzle select **with relation support** — the select-only counterpart
 * of `generatePaginationQuery`.
 *
 * Works with `SelectQueryPayload` (returned by `select()` from `zod-paginate`)
 * instead of the full `PaginationQueryParams`. Since `select()` only produces a
 * `select` array (no filters, sorting, or limit/offset), this function builds a
 * simpler set of queries.
 *
 * Relation-prefixed fields (e.g. `"posts.title"`) are **automatically routed**
 * to the corresponding relation query and **stripped from the main query**.
 *
 * After executing all queries, use `assembleDrizzleRelations` to reconstruct
 * nested objects (the assembler works identically for both flavours).
 *
 * @example
 * ```ts
 * const result = generateSelectQuery(parsed, {
 *   buildQuery: (select) => db.select(select).from(usersTable),
 *   fields: { id: usersTable.id, name: usersTable.name },
 *   relations: [
 *     {
 *       relationName: 'posts',
 *       fields: { id: postsTable.id, title: postsTable.title },
 *       foreignKey: postsTable.authorId,
 *       parentKey: usersTable.id,
 *       buildQuery: (select) => db.select(select).from(postsTable),
 *     },
 *   ],
 * });
 *
 * // One-call approach:
 * const { data } = await result.execute();
 *
 * // Or manual approach:
 * const [mainRows, ...relationRows] = await Promise.all([
 *   result.query,
 *   ...result.relationQueries.map((r) => r.query),
 * ]);
 * const data = result.assemble(mainRows, relationRows);
 * ```
 */
export function generateSelectQuery<
  TSchema extends DataSchema,
  TColumn extends DrizzleSqlColumn,
  TFields extends Record<string, TColumn>,
  const TRelations extends readonly AnyDrizzleRelation[],
>(
  parsed: SelectQueryPayload<TSchema> & { responseType: 'one' },
  config: {
    /** Factory that receives the generated select shape and must return a Drizzle query builder. */
    buildQuery: (
      selectShape: DrizzleSelectShape<NoInfer<TColumn>>,
    ) => DrizzleAutoQuery<InferFieldsData<NoInfer<TFields>>[]>;
    /** Map from allowed field paths to Drizzle column references. */
    fields: TFields & DrizzleFieldMap<TSchema, TColumn>;
    /** Relations to fetch as separate queries and assemble into parent rows. */
    relations?: TRelations;
    /** When `true` (default), throws if a requested field has no mapping in `fields`. */
    strictFieldMapping?: boolean;
    /** Custom alias generator for select keys. Defaults to replacing dots with underscores. */
    selectAlias?: (fieldPath: string) => string;
  },
): DrizzleSelectWithRelationsResult<TSchema, TFields, TRelations, 'one'>;

export function generateSelectQuery<
  TSchema extends DataSchema,
  TColumn extends DrizzleSqlColumn,
  TFields extends Record<string, TColumn>,
  const TRelations extends readonly AnyDrizzleRelation[],
>(
  parsed: SelectQueryPayload<TSchema>,
  config: {
    /** Factory that receives the generated select shape and must return a Drizzle query builder. */
    buildQuery: (
      selectShape: DrizzleSelectShape<NoInfer<TColumn>>,
    ) => DrizzleAutoQuery<InferFieldsData<NoInfer<TFields>>[]>;
    /** Map from allowed field paths to Drizzle column references. */
    fields: TFields & DrizzleFieldMap<TSchema, TColumn>;
    /** Relations to fetch as separate queries and assemble into parent rows. */
    relations?: TRelations;
    /** When `true` (default), throws if a requested field has no mapping in `fields`. */
    strictFieldMapping?: boolean;
    /** Custom alias generator for select keys. Defaults to replacing dots with underscores. */
    selectAlias?: (fieldPath: string) => string;
  },
): DrizzleSelectWithRelationsResult<TSchema, TFields, TRelations>;

export function generateSelectQuery<
  TSchema extends DataSchema,
  TColumn extends DrizzleSqlColumn,
  TFields extends Record<string, TColumn>,
  const TRelations extends readonly AnyDrizzleRelation[],
>(
  parsed: SelectQueryPayload<TSchema>,
  config: {
    /**
     * Factory that receives the generated select shape and must return a Drizzle
     * query builder (e.g. `db.select(select).from(table)`).
     */
    buildQuery: (
      selectShape: DrizzleSelectShape<NoInfer<TColumn>>,
    ) => DrizzleAutoQuery<InferFieldsData<NoInfer<TFields>>[]>;
    /**
     * Map from allowed field paths to Drizzle column references.
     */
    fields: TFields & DrizzleFieldMap<TSchema, TColumn>;
    /** Relations to fetch as separate queries and assemble into parent rows. */
    relations?: TRelations;
    /**
     * When `true` (default), throws if a requested field has no mapping in `fields`.
     * Set to `false` to silently ignore unmapped fields.
     */
    strictFieldMapping?: boolean;
    /**
     * Custom alias generator for select keys. Receives a dotted field path
     * (e.g. `"posts.title"`) and must return a valid SQL alias.
     * Defaults to replacing dots with underscores (`"posts_title"`).
     */
    selectAlias?: (fieldPath: string) => string;
  },
): DrizzleSelectWithRelationsResult<TSchema, TFields, TRelations, SelectResponseType> {
  const aliasBuilder = config.selectAlias ?? defaultSelectAlias;
  const strictFieldMapping = config.strictFieldMapping ?? true;
  // @ts-expect-error -- empty array is a valid runtime fallback for TRelations
  const relations: TRelations = config.relations ?? [];
  const relationNames = relations.map((r) => r.relationName);

  // ── Partition select paths ──────────────────────────────────────
  const mainSelect = parsed.fields.filter((fp) => !belongsToAnyRelation(`${fp}`, relationNames));

  // ── Build the main select shape ─────────────────────────────────
  const mainSelectShape = buildSelectShapeInternal(
    mainSelect.map(String),
    config.fields,
    strictFieldMapping,
    aliasBuilder,
  );

  // Inject parent key columns (needed for assembling).
  const parentKeyFields: DrizzleSelectShape<DrizzleSqlColumn> = {};
  for (const relation of relations) {
    const parentKeys = toArray(relation.parentKey);
    const pkAliases = collectAliases(buildPkAliases(relation.relationName, parentKeys));
    for (let i = 0; i < parentKeys.length; i++) {
      const alias = pkAliases[i];
      const col = parentKeys[i];
      if (alias !== undefined && col !== undefined) {
        parentKeyFields[alias] = col;
      }
    }
  }
  Object.assign(mainSelectShape, parentKeyFields);

  let query: DrizzleDynamicQuery = config.buildQuery(mainSelectShape).$dynamic();

  // When responseType is 'one', limit to a single row.
  if (parsed.responseType === 'one') {
    query = query.limit(1);
  }

  // Operators for building relation scope filters (eq / inArray are dialect-independent).
  const scopeOperators = createPgDrizzleOperators();

  // ── Build relation queries (select-only, no filters/sort) ───────
  const selectPaths = parsed.fields.map(String);
  const relationQueries = relations.map((relation) =>
    buildSelectOnlyRelationQuery(selectPaths, relation, aliasBuilder, strictFieldMapping),
  );

  // ── assemble / execute helpers ──────────────────────────────────
  type AssembledRow = InferAssembledRow<TFields, TRelations>;
  type ExecuteResult = SelectResponse<TSchema, AllowedSelectablePath<TSchema>, SelectResponseType>;

  const assemble = (
    mainRows: Record<string, unknown>[],
    relationResults: Record<string, unknown>[][],
  ): AssembledRow[] =>
    coerceAssembledRows<AssembledRow>(
      assembleDrizzleRelations(mainRows, relationQueries, relationResults),
    );

  const execute = async (): Promise<ExecuteResult | null> => {
    // Execute main query first to obtain parent IDs for relation scoping.
    const mainRows: Record<string, unknown>[] = await query;

    // Build scoped relation queries (filtered by parent IDs).
    const scopedQueries = relations.map((relation) => {
      const scope = buildRelationScope(
        mainRows,
        relation.relationName,
        relation.parentKey,
        relation.foreignKey,
        scopeOperators,
      );
      return buildSelectOnlyRelationQuery(
        selectPaths,
        relation,
        aliasBuilder,
        strictFieldMapping,
        scope,
      );
    });

    const scopedRelationResults =
      scopedQueries.length > 0 ? await Promise.all(scopedQueries.map((rq) => rq.query)) : [];

    const rows = coerceAssembledRows<AssembledRow>(
      assembleDrizzleRelations(mainRows, scopedQueries, scopedRelationResults),
    );

    if (parsed.responseType === 'one') {
      const row = rows[0];
      if (!row) return null;
      // @ts-expect-error -- InferAssembledRow is structurally compatible with SelectResponseData at runtime
      const data: ExecuteResult['data'] = row;
      return { data };
    }

    // @ts-expect-error -- InferAssembledRow[] is structurally compatible with SelectResponseData at runtime
    const data: ExecuteResult['data'] = rows;
    return { data };
  };

  return { query, relationQueries, assemble, execute };
}

/**
 * Identity helper that reinterprets assembled `Record<string, unknown>[]` rows
 * as a more specific `TRow[]` type.
 *
 * At runtime this is a no-op (returns the same reference). It exists so that
 * callers with known generics can bridge the gap between the untyped output
 * of `assembleDrizzleRelations` and their inferred assembled-row type
 * **without** using `as` assertions.
 */
function coerceAssembledRows<TRow extends Record<string, unknown>>(
  rows: Record<string, unknown>[],
): TRow[] {
  // Identity cast: rows are structurally TRow at runtime; we return the same
  // reference typed more narrowly. This helper centralises the single
  // unavoidable generic widening so call-sites remain assertion-free.
  // @ts-expect-error -- Record<string, unknown>[] is a runtime superset of TRow[]
  return rows;
}

/**
 * Assembles the results of the main query and its relation queries into
 * nested objects.
 *
 * For each parent row, looks up the matching child rows via the parent-key /
 * foreign-key link and attaches them as an array property.
 *
 * @param mainRows      - Results from the main pagination query.
 * @param relationQueries - The `relationQueries` array returned by
 *                          `generatePaginationQuery`.
 * @param relationResults - An array of result arrays, one per relation query,
 *                          **in the same order** as `relationQueries`.
 * @returns A new array of main rows with relation data attached.
 *
 * @example
 * ```ts
 * const assembled = assembleDrizzleRelations(mainRows, relationQueries, [postsRows]);
 * // => [{ id: 1, name: 'Alice', posts: [{ id: 10, title: 'Hello' }] }, …]
 * ```
 */
export function assembleDrizzleRelations(
  mainRows: Record<string, unknown>[],
  relationQueries: DrizzleRelationQuery<unknown>[],
  relationResults: Record<string, unknown>[][],
): Record<string, unknown>[] {
  if (relationQueries.length !== relationResults.length) {
    throw new Error(
      `Mismatch: ${relationQueries.length} relation queries ` +
        `but ${relationResults.length} result arrays`,
    );
  }

  // Pre-index child rows by their foreign key value for O(1) lookup.
  const indexedRelations = relationQueries.map((rq, i) => {
    const childRows = relationResults[i] ?? [];
    const grouped = new Map<unknown, Record<string, unknown>[]>();
    const fkAliasList = collectAliases(rq.foreignKeyAlias);

    for (const row of childRows) {
      if (!isKeyValid(rq.foreignKeyAlias, row)) continue;
      const fkValue = compositeKey(rq.foreignKeyAlias, row);

      // Build a clean child row without the internal FK alias(es).
      const cleanRow: Record<string, unknown> = {};
      for (const [key, value] of Object.entries(row)) {
        if (!fkAliasList.includes(key)) {
          cleanRow[key] = value;
        }
      }

      const existing = grouped.get(fkValue);
      if (existing) {
        existing.push(cleanRow);
      } else {
        grouped.set(fkValue, [cleanRow]);
      }
    }

    return { relationName: rq.relationName, grouped };
  });

  // Index by relation name for O(1) lookup per relation per row.
  const indexedByName = new Map<string, Map<unknown, Record<string, unknown>[]>>();
  for (const ir of indexedRelations) {
    indexedByName.set(ir.relationName, ir.grouped);
  }

  // Collect internal parent-key alias names so we can omit them from output.
  const pkAliasSet = new Set<string>();
  for (const rq of relationQueries) {
    // Rebuild the PK aliases to match what was injected into the select shape.
    const parentKeys = toArray(rq.parentKey);
    const pkAliases = collectAliases(buildPkAliases(rq.relationName, parentKeys));
    for (const a of pkAliases) pkAliasSet.add(a);
  }

  return mainRows.map((row) => {
    const result: Record<string, unknown> = {};

    // Copy all original keys except internal parent-key aliases.
    for (const [key, value] of Object.entries(row)) {
      if (!pkAliasSet.has(key)) {
        result[key] = value;
      }
    }

    // Attach relation data (array or single object depending on mode).
    for (const rq of relationQueries) {
      const parentKeys = toArray(rq.parentKey);
      const pkAliases = collectAliases(buildPkAliases(rq.relationName, parentKeys));
      const pkRow: Record<string, unknown> = {};
      for (const a of pkAliases) pkRow[a] = row[a];

      const firstPkAlias = pkAliases[0];
      if (firstPkAlias === undefined || !isKeyValid(firstPkAlias, pkRow)) {
        result[rq.relationName] = rq.mode === 'one' ? null : [];
        continue;
      }

      // Build the lookup key using the same compositeKey approach on PK aliases.
      const fkAliases = rq.foreignKeyAlias;
      let lookupKey: unknown;
      if (typeof fkAliases === 'string') {
        // Single FK — use the single PK value directly.
        const singlePkAlias = pkAliases[0];
        lookupKey = singlePkAlias !== undefined ? row[singlePkAlias] : undefined;
      } else {
        // Composite FK — JSON-serialize the tuple of PK values.
        lookupKey = JSON.stringify(pkAliases.map((a) => row[a]));
      }

      const grouped = indexedByName.get(rq.relationName);
      let children = grouped?.get(lookupKey) ?? [];

      // Apply per-parent limit when configured.
      if (rq.limit !== undefined && rq.limit > 0 && children.length > rq.limit) {
        children = children.slice(0, rq.limit);
      }

      result[rq.relationName] = rq.mode === 'one' ? (children[0] ?? null) : children;
    }

    return result;
  });
}

// ─── Response metadata helpers ──────────────────────────────────────────────

/**
 * Builds the `pagination` metadata for a `LIMIT_OFFSET` response.
 *
 * Requires `totalItems` (from a `COUNT(*)` query) and the original parsed
 * pagination params. Returns all the fields expected by
 * `LimitOffsetPaginationResponseMeta`.
 *
 * @example
 * ```ts
 * const totalItems = await db.select({ count: sql`count(*)` }).from(usersTable);
 * const meta = buildLimitOffsetResponseMeta(parsed, totalItems[0].count);
 * // => { itemsPerPage: 10, totalItems: 42, currentPage: 2, totalPages: 5 }
 * ```
 */
export function buildLimitOffsetResponseMeta<TSchema extends DataSchema>(
  parsed: PaginationPayload<TSchema, 'LIMIT_OFFSET'>,
  totalItems: number,
): LimitOffsetPaginationResponseMeta {
  const pagination = parsed;
  const safePage = typeof pagination.page === 'number' && pagination.page > 0 ? pagination.page : 1;
  const totalPages =
    pagination.limit > 0 ? Math.max(1, Math.ceil(totalItems / pagination.limit)) : 1;

  return {
    itemsPerPage: pagination.limit,
    totalItems,
    currentPage: safePage,
    totalPages,
    sortBy: pagination.sortBy?.map((s) => ({
      property: `${s.property}`,
      direction: s.direction,
    })),
    filter: pagination.filters,
  };
}

/**
 * Builds the `pagination` metadata for a `CURSOR` response.
 *
 * Extracts the cursor value from the last row of the result set so the client
 * can request the next page. If there are no rows, the incoming cursor is
 * returned as-is (or `0` as a fallback).
 *
 * @param parsed     - The parsed cursor-pagination params.
 * @param rows       - The rows returned by the main paginated query.
 * @param cursorField - The key in each row that holds the cursor value.
 *                      Defaults to `selectAlias(cursorProperty)` (or
 *                      `defaultSelectAlias` when no `selectAlias` is given).
 * @param selectAlias - Alias builder matching the one used to build the query.
 *                      When omitted, `defaultSelectAlias` is used.
 *
 * @example
 * ```ts
 * const meta = buildCursorResponseMeta(parsed, rows, 'id');
 * // => { itemsPerPage: 10, cursor: 42 }
 * ```
 */
export function buildCursorResponseMeta<TSchema extends DataSchema>(
  parsed: PaginationPayload<TSchema, 'CURSOR'>,
  rows: Record<string, unknown>[],
  cursorField?: string,
  selectAlias?: (fieldPath: string) => string,
): CursorPaginationResponseMeta {
  const pagination = parsed;
  const aliasBuilder = selectAlias ?? defaultSelectAlias;
  const resolvedCursorField = cursorField ?? aliasBuilder(`${pagination.cursorProperty}`);

  let nextCursor: number | string | Date;

  const lastRow = rows.length > 0 ? rows[rows.length - 1] : undefined;
  if (lastRow) {
    const rawValue = lastRow[resolvedCursorField];
    if (rawValue instanceof Date) {
      nextCursor = rawValue;
    } else if (typeof rawValue === 'number' || typeof rawValue === 'string') {
      nextCursor = rawValue;
    } else {
      // Fallback: use incoming cursor or 0.
      nextCursor = pagination.cursor ?? 0;
    }
  } else {
    nextCursor = pagination.cursor ?? 0;
  }

  return {
    itemsPerPage: pagination.limit,
    cursor: nextCursor,
    sortBy: pagination.sortBy?.map((s) => ({
      property: `${s.property}`,
      direction: s.direction,
    })),
    filter: pagination.filters,
  };
}
