import { sql } from 'drizzle-orm';
import { drizzle, MySql2Database } from 'drizzle-orm/mysql2';
import mysql from 'mysql2/promise';
import { afterAll, beforeAll, beforeEach } from 'vitest';
import * as schema from './schemas';

const DATABASE_URL = process.env.MYSQL_URL ?? 'mysql://test:test@localhost:3306/test';

let connection: mysql.Connection;

export let db: MySql2Database<typeof schema> & {
  $client: mysql.Connection;
};

export function setupMysql(): void {
  beforeAll(async () => {
    connection = await mysql.createConnection(DATABASE_URL);
    db = drizzle(connection, { schema, mode: 'default' });

    await db.execute(sql`
      CREATE TABLE IF NOT EXISTS users (
        id INT PRIMARY KEY AUTO_INCREMENT,
        name TEXT NOT NULL,
        email TEXT NOT NULL,
        age INT,
        status TEXT NOT NULL DEFAULT ('ACTIVE')
      )
    `);

    await db.execute(sql`
      CREATE TABLE IF NOT EXISTS posts (
        id INT PRIMARY KEY AUTO_INCREMENT,
        title TEXT NOT NULL,
        author_id INT REFERENCES users(id)
      )
    `);
  });

  beforeEach(async () => {
    await db.execute(sql`TRUNCATE TABLE posts`);
    await db.execute(sql`TRUNCATE TABLE users`);
  });

  afterAll(async () => {
    await db.execute(sql`DROP TABLE IF EXISTS posts`);
    await db.execute(sql`DROP TABLE IF EXISTS users`);
    await connection.end();
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
