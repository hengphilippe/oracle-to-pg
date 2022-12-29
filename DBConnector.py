# Standard Library
import logging as logger
import time
import json
# Third Party Libraries
import cx_Oracle
from datetime import datetime

# Message Model
from Producer.message import Message
from Subscribe import batchLoad as Subc


class DBConnector:
    def __init__(self, name, dsn, username, passwd) -> None:
        self.name = name
        self.dsn = dsn
        self.username = username
        self.passwd = passwd
        self.encoding = "UTF-8"
        try:
            self.pool = cx_Oracle.SessionPool(
                self.username,
                self.passwd,
                self.dsn,
                min=2,
                max=5,
                increment=3,
                getmode=cx_Oracle.SPOOL_ATTRVAL_TIMEDWAIT,
                encoding=self.encoding
            )
            # Acquire a connection from the pool
            connection = self.pool.acquire()
            self.connection = connection
            self.cursor = self.connection.cursor()
            self.cursor.execute(
                "ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD HH24:MI:SS' NLS_TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF'")

            # print("connected to : ", connection.version)
            logger.info(f"Connected to {connection.version} database")
        except cx_Oracle.DatabaseError as e:
            print("There is a problem with Oracle", e)
        # finally:
            # print("init session...")

    def query(self, sql, bind_variables=None):
        try:
            conn = self.pool.acquire()
            cursor = conn.cursor()
            if bind_variables is None:
                logger.debug(f"Query: {sql}")
                cursor.execute(sql)
            else:
                logger.debug(f"Query: {sql} value: {bind_variables}")
                cursor.execute(sql, bind_variables)
            columns = [description[0] for description in cursor.description]
            results = []
            for row in cursor:
                results.append(dict(zip(columns, row)))
            self.pool.release(conn)
            return results
        except Exception as err:
            logger.exception(err)

    def getCurrentSCN(self):
        if(self.connection):
            conn = self.pool.cursor()
            cursor = conn.cursor()
            query = cursor.execute('select current_scn from v$database')
            result = query.fetchone()
            cursor.close()
        return result[0]

    def output_type_handler(self, cursor, name, default_type, size, precision, scale):
        if default_type == cx_Oracle.DB_TYPE_CLOB:
            return self.cursor.var(cx_Oracle.DB_TYPE_LONG, arraysize=self.cursor.arraysize)
        if default_type == cx_Oracle.DB_TYPE_BLOB:
            return self.cursor.var(cx_Oracle.DB_TYPE_LONG_RAW, arraysize=self.cursor.arraysize)

    def dumpTable(self, table, Sub_Conn):
        records = []
        getTable = table.split(".")[1]
        getSegOwner = table.split(".")[0]

        # conn = self.pool.acquire()

        query = self.cursor.execute('select current_scn from v$database')
        currentSCN = query.fetchone()
        SCN = int(currentSCN[0])

        self.cursor.prefetchrows = 50000
        self.cursor.arraysize = 50000
        sql = "SELECT * FROM {0}".format(table)
        #sql = "select * from {0} where instanceid>4158439".format(table)
        #sql = 'SELECT * FROM (select * from {0} order by instanceid desc) WHERE  instanceid in (4156664,4156652,4156657,4157326,4156656,4156654,4156653,4156663)'.format(table)
        # subcriber = Subc.Subcriber()
        try:
            self.connection.outputtypehandler = self.output_type_handler
            query = self.cursor.execute(sql)
            # res = cursor.fetchall()
            row_headers = [column[0] for column in query.description]
            while True:
                rows = self.cursor.fetchmany()
                print(len(rows))
                if rows:
                    records = []
                    for row in rows:
                        record = dict(zip(row_headers, row))
                        
                        message = Message(
                            scn=0,
                            seg_owner=getSegOwner,
                            table_name=getTable,
                            sql_redo=sql,
                            operation="READ",
                            data=record,
                            timestamp=int(time.time())
                        )
                        # add all to records
                        records.append(json.dumps(message.dict(), default=str))
                    
                    # print("retrived {0} from oracle".format(
                    #     self.cursor.prefetchrows))
                    start_time = datetime.now()
                    # Sub_Conn.insert_v2(table, json.dumps(records, default=str), start_time)
                    Sub_Conn.insert_v2(table, records, start_time)
                    print("Finish one round....")

                if len(rows) < self.cursor.arraysize:
                    break
            # Get header
            # row_headers = [column[0] for column in query.description]
            # Get records
            # for row in res:
                # record = dict(zip(row_headers, row))

                # message = Message(
                #     scn=0,
                #     seg_owner=getSegOwner,
                #     table_name=getTable,
                #     sql_redo=sql,
                #     operation="READ",
                #     data=record,
                #     timestamp=int(time.time()))
                # # add all to records
                # records.append(message.dict())

            # Return all records
            return SCN
            # return json.dumps(records,default=str)
        except Exception as err:
            logger.exception(err)
        # finally:
        #     self.cursor.close()

    def callLogMining(self, startSCN, schemas, tables):
        """
        starting logminer session by SCN start , end
        :startSCN:
        :endSCN : dafault current SCN
        """
        conn = self.pool.acquire()
        cursor = conn.cursor()

        # query = cursor.execute('select current_scn from v$database')
        query = cursor.execute('select max(next_change#) from V$ARCHIVED_LOG')
        lastLogSCN = query.fetchone()
        SCN = int(lastLogSCN[0])

        if SCN <= int(startSCN):
            return [], startSCN
        else:
            # enable DBMS_OUTPUT
            endSCN = cursor.var(int)
            cursor.callproc('gdce.dbfeeds', [endSCN, 2, startSCN])
            """
            SELECT * FROM v$LOGMNR_CONTENTS WHERE TABLE_NAME IN (list) AND SCHEMA IN (list)
            """
            # print(endSCN.getvalue())
            # bindSchemas = [":" + str(i + 1) for i in range(len(schemas))]
            bind_tables = [":" + str(i + 1) for i in range(len(tables))]
            sql = "select scn, row_id, csf,seg_owner,table_name, operation, sql_redo from v$logmnr_contents" + \
                " where table_name in (%s)" % (
                    ", ".join(bind_tables))
            # "and table_name in (%s)" % (", ".join(tables))
        
            cursor.execute(sql, tables)
            result = cursor.fetchall()
            # print(result)
            
            # end logminer
            cursor.callproc("DBMS_LOGMNR.end_logmnr")
            cursor.close()
            return result, endSCN.getvalue()


if __name__ == "__main__":
    dbConn = DBConnector("ASYWDB",
                         "10.0.10.43/DEVDB",
                         "lipz",
                         "lipz#")
    tables = ['customers', 'logon', 'orders']
    schemas = ['soe']
    records, scn = dbConn.callLogMining(12501057, 'SOE', ['customers', 'demo'])
    if len(records) > 0:
        for scn, row_id, csf, table_name, operation, sql_redo in records:
            print(scn, table_name)
    else:
        print("No records found :P")
