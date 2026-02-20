import { sql } from 'drizzle-orm';
import { drizzle, NodePgDatabase } from 'drizzle-orm/node-postgres';
import pg from 'pg';
import { afterAll, beforeAll, beforeEach } from 'vitest';
import * as schema from './schemas';

const DATABASE_URL = process.env.PG_URL ?? 'postgres://test:test@localhost:5432/test';

let pool: pg.Pool;

export let db: NodePgDatabase<typeof schema> & {
  $client: pg.Pool;
};

export function setupPg(): void {
  beforeAll(async () => {
    pool = new pg.Pool({ connectionString: DATABASE_URL });
    db = drizzle(pool, { schema });

    await db.execute(sql`
      CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
        name TEXT NOT NULL,
        email TEXT NOT NULL,
        age INTEGER,
        status TEXT NOT NULL DEFAULT 'ACTIVE'
      )
    `);

    await db.execute(sql`
      CREATE TABLE IF NOT EXISTS posts (
        id INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
        title TEXT NOT NULL,
        author_id INTEGER REFERENCES users(id)
      )
    `);
  });

  beforeEach(async () => {
    await db.execute(sql`TRUNCATE TABLE posts RESTART IDENTITY CASCADE`);
    await db.execute(sql`TRUNCATE TABLE users RESTART IDENTITY CASCADE`);
  });

  afterAll(async () => {
    await db.execute(sql`DROP TABLE IF EXISTS posts`);
    await db.execute(sql`DROP TABLE IF EXISTS users`);
    await pool.end();
  });
}

export async function seedUsers(): Promise<void> {
  await db.insert(schema.users).values([
    { name: 'Alice', email: 'alice@test.com', age: 30, status: 'ACTIVE' },
    { name: 'Bob', email: 'bob@test.com', age: 25, status: 'ACTIVE' },
    { name: 'Charlie', email: 'charlie@test.com', age: 35, status: 'INACTIVE' },
    { name: 'Diana', email: 'diana@test.com', age: 28, status: 'ACTIVE' },
    { name: 'Eve', email: 'eve@test.com', age: 22, status: 'BANNED' },
  ]);
}
