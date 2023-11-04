require('dotenv').config()

const mysql2 = require("mysql2/promise");
const { Client } = require("pg");
const fs = require("fs");

const env = process.env

// Define a mapping between MySQL data types and equivalent PostgreSQL data types
const dataTypeMapping = {
  int: "integer",
  smallint: "smallint",
  mediumint: "integer",
  bigint: "bigint",
  tinyint: "smallint",
  bit: "bit",
  bool: "boolean",
  boolean: "boolean",
  enum: "varchar",
  set: "varchar",
  char: "char",
  varchar: "varchar",
  text: "text",
  tinytext: "text",
  mediumtext: "text",
  longtext: "text",
  binary: "bytea",
  varbinary: "bytea",
  blob: "bytea",
  tinyblob: "bytea",
  mediumblob: "bytea",
  longblob: "bytea",
  date: "date",
  datetime: "timestamp",
  timestamp: "timestamp",
  time: "time",
  year: "integer",
  float: "real",
  double: "double precision",
  decimal: "numeric",
  numeric: "numeric",
  real: "real",
  "double precision": "double precision",
  serial: "serial",
  bigserial: "bigserial",
  json: "json",
  jsonb: "jsonb",
  uuid: "uuid",
  xml: "xml",
  money: "money",
  point: "point",
  line: "line",
  lseg: "lseg",
  box: "box",
  path: "path",
  polygon: "polygon",
  circle: "circle",
  cidr: "cidr",
  inet: "inet",
  macaddr: "macaddr",
  tsvector: "tsvector",
  tsquery: "tsquery",
  int4range: "int4range",
  int8range: "int8range",
  numrange: "numrange",
  tsrange: "tsrange",
  tstzrange: "tstzrange",
  daterange: "daterange",
  geometry: "geometry",
  geography: "geography",
  // Add more mappings as needed
};

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

  const createTableInPostgreSQL = async (tableName, columns) => {
    try {
      // Map MySQL data types to PostgreSQL data types and adjust length for varchar
      const mappedColumns = columns.map((column) => {
        const [columnName, dataType] = column.split(" ");
        let dataTypeWithoutSize = dataType;
        let length = "";

        if (dataType.includes("(")) {
          const sizeMatch = dataType.match(/\d+/);
          const size = sizeMatch ? sizeMatch[0] : null;
          dataTypeWithoutSize = dataType.split("(")[0];
          if (dataTypeWithoutSize.toLowerCase() === "varchar") {
            length = size > 255 ? "(255)" : dataType.match(/\(\d+\)/)[0];
          }
        }

        // Check if the column name includes "is_" and set type to boolean
        const mappedDataType = columnName.includes("is_", "active")
          ? "boolean"
          : dataTypeMapping[dataTypeWithoutSize.toLowerCase()] ||
            dataTypeWithoutSize;

        return `"${columnName}" ${mappedDataType}${length}`;
      });

      const createQuery = `CREATE TABLE ${tableName} (${mappedColumns.join(
        ", "
      )})`;

      // Write the query to an SQL file
      const fileName = `./sql-generations/${tableName}_create_table.sql`;
      fs.writeFileSync(fileName, createQuery);

      await pgClient.query(createQuery);
      console.log(`Table '${tableName}' created in PostgreSQL.`);
    } catch (error) {
      throw error;
    }
  };

  const getTableColumns = async (connection, tableName) => {
    try {
      if (connection.constructor.name === "PromiseConnection") {
        // This is a MySQL connection
        const [columns] = await connection.query(`DESCRIBE ${tableName}`);
        return columns.map((column) => `${column.Field} ${column.Type}`);
      } else if (connection.constructor.name === "Client") {
        // This is a PostgreSQL connection
        const { rows } = await connection.query(
          "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = $1",
          [tableName]
        );
        return rows.map((row) => `${row.column_name} ${row.data_type}`);
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
        // console.log(mysqlColumns);
        await createTableInPostgreSQL(tableName, mysqlColumns);
      }
    }

    await mysqlConnection.end();
    pgClient.end();
  } catch (error) {
    console.error("Error:", error);
  }
}

main();
