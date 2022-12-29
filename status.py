# Configuration
from Configuration import ConfigHandler
from Configuration import TableHandler as TablesSCN

# Database
import cx_Oracle
from DBConnector import DBConnector

import psycopg2
from Subscribe import batchLoad

def task(source) :
    print("-> Task for : ", source)
    connector = ConfigHandler.Connector()
    databaseConnection = connector.config_data[source]

    pgConn = batchLoad.Subcriber()

    # load json configuration to ini file
    # connector.loadNewTables(source)

    OraTables = TablesSCN.TableHandler(source)
    # init connection to database
    dbConn = DBConnector(databaseConnection["name"],
                         databaseConnection["dsn"],
                         databaseConnection["username"],
                         databaseConnection["password"])

    # print(OraTables.getTables())
    printReport("Tables","Oracle", "Postges", "Status")
    print("---------------------------------------------------------------------------")
    for table in OraTables.getTables() :
      status = "✅"
      pgTable = table.split(".")[1]

      ora_sql = f"select count(1) as REC from {table}"
      pg_sql = f"select count(1) as REC from {pgTable}"
      OracleResult = dbConn.query(ora_sql)
      pgConn.cursor.execute(pg_sql)
      PostgreSQLResult = pgConn.cursor.fetchone();
      ora_count = OracleResult[0]["REC"]
      pg_count = PostgreSQLResult[0]

      if(ora_count != pg_count) : 
        status = "⛔"
      printReport(table,ora_count,pg_count,status)

def printReport(table,ora_count,pg_count,status) :
  print(f"{table:>40} | {ora_count:>10} | {pg_count:>10} | {status}")


if __name__ == "__main__":
    for source in ["ASYWDB", "ECUSDB"]:
      task(source)
