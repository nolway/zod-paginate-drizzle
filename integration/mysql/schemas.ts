import { int, mysqlTable, text } from 'drizzle-orm/mysql-core';

export const users = mysqlTable('users', {
  id: int('id').primaryKey().autoincrement(),
  name: text('name').notNull(),
  email: text('email').notNull(),
  age: int('age'),
  status: text('status').notNull().default('ACTIVE'),
});

export const posts = mysqlTable('posts', {
  id: int('id').primaryKey().autoincrement(),
  title: text('title').notNull(),
  authorId: int('author_id').references(() => users.id),
});
