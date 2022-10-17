import * as dotenv from "dotenv";
import { Kysely, MysqlDialect, RawBuilder, sql } from "kysely";
import type { DB } from "kysely-codegen";
import { createPool } from "mysql2";

dotenv.config();

export const db = new Kysely<DB>({
  dialect: new MysqlDialect({
    pool: createPool({ decimalNumbers: true, uri: process.env.DATABASE_URL }),
  }),
  log(event) {
    if (event.level === "query") {
      console.log(event.query.sql, event.query.parameters);
    }
  },
});

type Table = keyof DB;

export const getTableRef = (table: Table): RawBuilder<Table> =>
  sql.table(table).castTo<Table>();
