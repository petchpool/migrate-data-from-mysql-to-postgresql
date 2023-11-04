require('dotenv').config()

const mysql = require("mysql2/promise");
const moment = require("moment");
const { Pool } = require("pg");
const fs = require("fs");

const env = process.env

// MySQL Pool
const mysqlPool = mysql.createPool({
  host: env.MYSQL_HOST,
  port: env.MYSQL_PORT,
  user: env.MYSQL_USER,
  password: env.MYSQL_PASSWORD,
  database: env.MYSQL_DATABASE,
  waitForConnections: true,
  connectionLimit: 10,
  queueLimit: 0,
});

// PostgreSQL Pool
const pgPool = new Pool({
  host: env.PG_HOST,
  port: env.PG_PORT,
  user: env.PG_USER,
  password: env.PG_PASSWORD,
  database: env.PG_DATABASE,
});

let mysqlConnection, pgClient;
let successInsert = 0;

const batchSize = 10000; // Number of rows to insert per batch

// Function to load completed tables from file
function loadCompletedTables() {
  try {
    const data = fs.readFileSync("completedTables.json");
    return JSON.parse(data);
  } catch (error) {
    return { completedTables: [] };
  }
}

// Function to save completed tables to file
function saveCompletedTables(completedTables) {
  const data = JSON.stringify(completedTables);
  fs.writeFileSync("completedTables.json", data);
}

async function getTableNames() {
  const mysqlConnection = await mysqlPool.getConnection();

  try {
    const [rows] = await mysqlConnection.execute(`
      SELECT 
          table_name AS \`Table Name\`,
          table_rows AS \`Number of Rows\`,
          ROUND(data_length / ( 1024 * 1024 ), 2) AS \`Data Size (MB)\`
      FROM 
          information_schema.tables
      WHERE 
          table_schema = 'platform_prod'
      ORDER BY 
          table_rows ASC;
    `);
    return rows.map((row) => row[`Table Name`]);
  } finally {
    await mysqlConnection.release();
  }
}

async function transferData(tableName) {
  try {
    mysqlConnection = await mysqlPool.getConnection();

    const [totalRows] = await mysqlConnection.execute(
      `SELECT COUNT(*) as count FROM ${tableName}`
    );

    const totalPages = Math.ceil(totalRows[0].count / batchSize);

    await mysqlConnection.release();

    pgClient = await pgPool.connect();

    const truncateQuery = `TRUNCATE TABLE ${tableName};`;
    await pgClient.query(truncateQuery);

    for (let page = 1; page <= totalPages; page++) {
      const offset = (page - 1) * batchSize;

      const [rows] = await mysqlConnection.execute(
        `SELECT * FROM ${tableName} LIMIT ${batchSize} OFFSET ${offset}`
      );

      let batchInsertValues = [];

      for (const row of rows) {
        const columns = Object.keys(row);
        const values = columns.map((col) => row[col]);
        batchInsertValues.push(values);
      }

      if (batchInsertValues.length > 0) {
        const columns = Object.keys(rows[0]);
        try {
          await performBatchInsert(
            pgClient,
            tableName,
            columns,
            batchInsertValues
          );
        } catch (error) {
          throw error;
        }
      }
    }
  } finally {
    if (mysqlConnection) {
      await mysqlConnection.release();
    }

    if (pgClient) {
      pgClient.release();
    }
  }
}

async function performBatchInsert(pgClient, tableName, columns, valuesArray) {
  let insertQuery = `INSERT INTO "${tableName}" ("${columns.join(
    '", "'
  )}") VALUES`;

  // let insertQuery = `INSERT INTO "${tableName}" VALUES`;
  const formattedValues = valuesArray
    .map(
      (row) =>
        `(${row
          .map((val) => {
            if (val == null) {
              return "NULL";
            } else if (val instanceof Date) {
              return `'${moment(val).format("YYYY-MM-DD HH:mm:ss.SSS")}'`;
            } else if (typeof val === "object") {
              const rawString = `${JSON.stringify(val)}`
              const formatString = rawString.replace(/'/g, "");
              return `'${formatString}'` 
            } else if (typeof val === "string") {
              return `'${val.replace(/['"]/g, " ")}'`;
            } else {
              return `'${val}'`;
            }
          })
          .join(", ")})`
    )
    .join(",\n");

  if (formattedValues.length === 0) {
    console.error("No valid values to insert.");
    return;
  }

  insertQuery += `\n${formattedValues}`;

  try {
    const start = Date.now();

    await pgClient.query(insertQuery);

    const end = Date.now();
    const elapsedTime = end - start;
    const elapsedTimeInSeconds = (elapsedTime / 1000).toFixed(3);
    successInsert += valuesArray.length;
    console.log(
      `Successfully inserted`,
      valuesArray.length,
      `/`,
      successInsert,
      `rows in`,
      elapsedTime,
      `ms`,
      `(`,
      elapsedTimeInSeconds,
      `seconds)`
    );
    await new Promise((resolve) => setTimeout(resolve, 2000));
  } catch (error) {
    fs.writeFileSync(`./errors/error-insert-${tableName}.sql`, insertQuery);
    console.log(insertQuery.substring(0, 3000));
    throw error;
  }
}

async function startDataTransfer() {
  const completedTables = loadCompletedTables();
  const tableNames = await getTableNames();

  for (const tableName of tableNames) {
    if (!completedTables.completedTables.includes(tableName)) {
      try {
        console.log(`Table: '${tableName}' is being transferred...`);
        await transferData(tableName);
        successInsert = 0;
        console.log(`Data transfer for '${tableName}' complete`);
        console.log(`\n*********************************\n`);
        await new Promise((resolve) => setTimeout(resolve, 2000));
        completedTables.completedTables.push(tableName);
        saveCompletedTables(completedTables);
      } catch (error) {
        console.error(
          `\n--> Error: transferring data for '${tableName}' -->`,
          error.message
        );
        console.log(`\n*********************************\n`);
        continue;
      }
    } else {
      console.log(`Table '${tableName}' already completed. Skipping...`);
      console.log(`\n*********************************\n`);
    }
  }
}

startDataTransfer()
  .then(async () => {
    await mysqlPool.end();
    await pgPool.end();
    console.log("Data transfer complete");
  })
  .catch((err) => console.error("Error:", err));
