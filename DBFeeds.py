#!/usr/local/bin/python3.9

import json
from datetime import datetime
from time import sleep
import psycopg2
import sys

from numpy import single
# Configuration
from Configuration import ConfigHandler
from Configuration import TableHandler as TablesSCN

# Database
from DBConnector import DBConnector

# SQL statement Parser
from DMLParser import insertParser, updateParser, deleteParser
# Kafka
# from Producer import kafkaTopic as KafkaTopic
from Subscribe import batchLoad as Subc


#def handleKafkaTopic(source):
#    return 0


def task(source):
    print("---------------------------")
    print(" -> Task for : ", source)
    connector = ConfigHandler.Connector()
    databaseConnection = connector.config_data[source]

    # load json configuration to ini file
    connector.loadNewTables(source)
    # get table with zero SCN
    ########################
    tables = TablesSCN.TableHandler(source)

    # kafkaTopics = tables.getTables(source)
    # kafkaServer = KafkaTopic.Server()
    # print(kafkaTopics)
    # exit(0)
    # kafkaServer.createTopic(source, kafkaTopics)
    # for kafkaTopic in kafkaTopics:
    #     name = source.upper() + kafkaTopic.upper()
    #     topic = kafkaServer.createTopic(name)

    tablesZeroSCN = tables.getZeroSCN()

    # init connection to database
    dbConn = DBConnector(databaseConnection["name"],
                         databaseConnection["dsn"],
                         databaseConnection["username"],
                         databaseConnection["password"])

    subcriber = Subc.Subcriber()

    # fresh read all record
    # ==============================
    if(len(tablesZeroSCN) > 0):
        for table in tablesZeroSCN:
            print("---------------------------")
            print("-> DUMP Table : ", table)
            scn = dbConn.dumpTable(table, subcriber)
            print(" ++ new scn will be : ", scn)
            tables.setSCN(table, str(scn))

    # continues mining
    # ==============================
    else:
        print("------------ Log mining process ---------------")

        # Declare empty string for store statement
        stmt = ""
        isContinue = False
        last_row_id = ""
        last_table_name = ""
        last_operation = ""
        # isNotEnd = 0

        # let min last SCN
        lastMinSCN = tables.getMinSCN()
        # Get table list with schema
        schemas_list, tables_list = tables.getAllSchemas_Tables()

        list_Of_STMT = []

        # Call logminer package
        records, currentSCN = dbConn.callLogMining(
            lastMinSCN, schemas_list, tables_list)

        if len(records) > 0:
            for scn, row_id, csf, seg_owner, table_name, operation, sql_redo in records :
                if (csf == 1):
                    if (isContinue == False):
                        stmt = sql_redo
                        isContinue = True
                    else:
                        stmt += sql_redo
                    continue
                else:
                    if(isContinue == True):
                        stmt += sql_redo
                    else:
                        stmt = sql_redo
                    isContinue = False

                topicName = source.upper() + "."+seg_owner.upper() + "." + table_name.upper()
                # Running extract SQL statement
                if operation == 'INSERT':
                    print("Insert : ", table_name)
                    insert_stmt = insertParser(
                        scn, stmt, seg_owner, table_name, operation)
                    if insert_stmt == None :
                        print("error inserting : ", stmt)
                    else : 
                        list_Of_STMT.append(insert_stmt)

                elif operation == 'UPDATE':
                    print("Update : ", table_name)
                    update_stmt = updateParser(
                        scn, stmt, seg_owner, table_name, operation)
                    if update_stmt == None :
                        print("error updating : ", stmt)
                    else :
                        list_Of_STMT.append(update_stmt)

                elif operation == 'DELETE':
                    print("Delete : ", table_name)
                    delete_stmt = deleteParser(
                        scn, stmt, seg_owner, table_name, operation
                    )
                    if delete_stmt == None:
                        print("error delete : ", stmt)
                    else : 
                        list_Of_STMT.append(delete_stmt) 

                else:
                    print(" -> Operator not found...")

            # print(list_Of_STMT)

            ## execute sttms to postgresql
            try :
                con = psycopg2.connect(
                    database="sale_entry",
                    user='postgres',
                    password='[12]root',
                    host='10.0.10.41',
                    port='5432'
                )
                cur = con.cursor()
                for stmt in list_Of_STMT : 
                    cur.execute(stmt)
                con.commit()
                tables.archive()
                tables.setSCNAll(str(currentSCN))
            except psycopg2.DatabaseError as e:
                if con:
                    con.rollback()

                print(f'Error {e}')
                sys.exit(1)

            finally:
                if con:
                    con.close()    
        
        else:
            print("No records found")
        # tables.archive()
        # tables.setSCNAll(str(currentSCN))


if __name__ == "__main__":
    # while(True):
    t1 = datetime.now()
    for source in ["ASYWDB", "ECUSDB"]:
        task(source)
    t2 = datetime.now()
    x = t2 - t1
    print(f"time was {x}")
    print("sleeping...")
        # sleep(10)
