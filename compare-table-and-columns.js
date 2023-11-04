require('dotenv').config()

const mysql2 = require("mysql2/promise");
const { Client } = require("pg");

const env = process.env

async function main() {
  // MySQL Connection
  const mysqlConnection = await mysql2.createConnection({
    host: env.MYSQL_HOST,
    port: env.MYSQL_PORT,
    user: env.MYSQL_USER,
    password: env.MYSQL_PASSWORD,
    database: env.MYSQL_DATABASE,
  });

  // PostgreSQL Connection
  const pgClient = new Client({
    host: env.PG_HOST,
    port: env.PG_PORT,
    user: env.PG_USER,
    password: env.PG_PASSWORD,
    database: env.PG_DATABASE,
  });

  const connectPostgreSQL = () => {
    return new Promise((resolve, reject) => {
      pgClient
        .connect()
        .then(() => {
          resolve();
        })
        .catch((err) => reject(err));
    });
  };

  const getTableColumns = async (connection, tableName) => {
    try {
      if (connection.constructor.name === "PromiseConnection") {
        // This is a MySQL connection
        const [columns] = await connection.query(`DESCRIBE ${tableName}`);
        return columns.map((column) => column.Field);
      } else if (connection.constructor.name === "Client") {
        // This is a PostgreSQL connection
        const { rows } = await connection.query(
          "SELECT column_name FROM information_schema.columns WHERE table_name = $1",
          [tableName]
        );
        return rows.map((row) => row.column_name);
      } else {
        throw new Error("Unknown connection type");
      }
    } catch (error) {
      throw error;
    }
  };

  try {
    await connectPostgreSQL();

    // Get list of tables from MySQL
    const [mysqlTables] = await mysqlConnection.query("SHOW TABLES");
    const mysqlTableNames = mysqlTables.map(
      (table) => table.Tables_in_platform_prod
    );

    // Get list of tables from PostgreSQL
    const { rows: pgTables } = await pgClient.query(
      "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"
    );
    const pgTableNames = pgTables.map((table) => table.table_name);

    // Compare columns for each table
    for (const tableName of mysqlTableNames) {
      console.log(
        `------------------- Comparing table '${tableName}' -------------------`
      );
      const mysqlColumns = await getTableColumns(mysqlConnection, tableName);
      if (pgTableNames.includes(tableName)) {
        const pgColumns = await getTableColumns(pgClient, tableName);

        const columnsNotInPostgres = mysqlColumns.filter(
          (column) => !pgColumns.includes(column)
        );

        if (columnsNotInPostgres.length > 0) {
          console.log(
            `Columns not present in PostgreSQL table '${tableName}': ${columnsNotInPostgres.join(
              ", "
            )}`
          );
        } else {
          console.log(
            `All columns are present in both tables for '${tableName}'.`
          );
        }
      } else {
        console.log(`Table '${tableName}' does not exist in PostgreSQL.`);
      }
    }

    await mysqlConnection.end();
    pgClient.end();
  } catch (error) {
    console.error("Error:", error);
  }
}

main();
