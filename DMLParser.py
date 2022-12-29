##
# Author  : Philippe.HENG
# Desc    : parser SQL statement to objects
# Date    : 05-Jul-2020
##

# Message Model
from pydoc_data.topics import topics
from Producer.message import Message
import cx_Oracle
# Message Model
from Producer.message import Message
import time
import json

# Kafka
#from Producer import kafkaTopic as KafkaTopic
#from kafka.producer import KafkaProducer

# Postgres DB
from Subscribe import batchLoad as Subc

# we can use select * from LIPZ.VIEW_LOGMINER

subcriber = Subc.Subcriber()


def insertParser(scn, sql, seg_owner, table, operation):
    data = {}
    data["value"] = {}
    data["where"] = {}

    addValuesIndex = sql.find("values\n    ")
    # print(valuesIndex)

    # *** Insert Values Zone ****
    ##
    # Author  : Philippe.HENG
    # Date    : 11-Jul-2020
    ##
    addValuesString = sql[addValuesIndex+6:len(sql)]
    addValuesString = addValuesString.replace("IS NULL", "= NULL")
    addValues = addValuesString.split(",\n    ")

    for addValue in addValues:
        splitIndex = addValue.find(" = ")
        startIndex = addValue.find('"')
        fieldKey = addValue[startIndex+1:splitIndex-1]
        fieldValue = addValue[splitIndex+3:len(addValue)]
        if fieldValue.find("'") == 0:
            fieldValue = fieldValue[1:-1]
            fieldValue = fieldValue.replace("'", "''")
            fieldValue = "'" + fieldValue + "'"
        # print(repr(fieldValue))
        # fieldValue = fieldValue.replace("'", "")
        # print("=> ", repr(fieldKey), " is ", repr(fieldValue))

        # if "HEXTORAW(" in fieldValue :
        #     fieldValue = fieldValue.replace("HEXTORAW(","")
        #     fieldValue = fieldValue[:-1]

        data["value"][fieldKey] = fieldValue
        # print(repr(fieldValue)
    # print(data['value'])

    message = subcriber.singleInsert(table, data["value"])
    # message = Message(scn=scn, seg_owner=seg_owner,
    #                   table_name=table, sql_redo=sql, operation=operation, data=data, timestamp=int(time.time()))
    # return json.dumps(message)
    # return message.json().encode('utf-8')

    return message


def insertParser_v2(scn, sql, seg_owner, table, operation):
    print(repr(sql))
    return True


def updateParser_v2(scn, sql, seg_owner, table, operation):
    print(repr(sql))
    return True


def updateParser(scn, sql, seg_owner, table, operation):
    # getTable = table.split(".")[1]
    # getSegOwner = table.split(".")[0]
    data = {}
    data["value"] = {}
    data["where"] = {}
    setIndex = sql.find("set\n")
    whereIndex = sql.find("where \n")

    # *** Set Values Zone ****
    ##
    # Author  : Philippe.HENG
    # Date    : 07-Jul-2020
    ##
    setValueString = sql[setIndex+5:whereIndex]

    # ** We split set value by ', \n    '
    # setValueString look like : '   "GDS_DSC" = \'- - Other\', \n    "TXT_RSV" = NULL, \n    "FLP1" = NULL\n  '
    setValues = setValueString.split(", \n    ")

    # For multi set key
    if(len(setValues) > 1):
        # print(" -> ", repr(setValueString))
        for setField in setValues:
            setField = setField.replace("\'", "")
            spliterIndx = setField.find(" = ")

            fieldKey = setField[0:spliterIndx-1]
            fieldKey = fieldKey.replace('"', '')
            fieldKey = fieldKey.strip()

            fieldValue = setField[spliterIndx+3:len(setField)]
            fieldValue = fieldValue.strip()
            data["value"][fieldKey] = fieldValue
            # print(" ++", repr(setField.strip()), "++")
            # print(fieldKey, " => ", fieldValue)
        # print(data["value"])
    else:
        # print(" -> ", repr(setValueString))
        spliterIndx = setValueString.find(" = ")

        fieldKey = setValueString[0:spliterIndx]
        fieldKey = fieldKey.replace('"', '')
        fieldKey = fieldKey.strip()
        fieldValue = setValueString[spliterIndx+3: len(setValueString)]
        fieldValue = fieldValue.replace("\'", "")
        fieldValue = fieldValue.strip()
        data["value"][fieldKey] = fieldValue

    # print(json.dumps(message.data))

    # *** Where Zone ****
    ##
    # Author  : Philippe.HENG
    # Date    : 07-Jul-2020
    ##
    whereValueString = sql[whereIndex+8:len(sql)]
    # ** We split set value by ' and \n    '
    # whereValueString look like :
    # '   "IED_ID" = 33525524 and \n
    # "DOC_VER" = 43 and \n
    # "OP_NAME" = \'Update\' and \n
    # "STATUS" = \'Validated\' and \n
    # "END_USER" = 2 and \n
    # "OWNER" IS NULL and \n
    # "CLOSED_DATE_TIME" IS NULL and \n
    # "OP_DATE_TIME" = \'05-MAY-22 11.11.15.674000 PM\' and \n
    # "C_BINDER_ID" = \'un.asybrk B_Selectivity_Lists\';'

    whereValueString = whereValueString.replace(" IS NULL", " = NULL")
    whereValues = whereValueString.split(" and \n    ")
    # print(repr(whereValueString))
    # For multi set key
    if(len(whereValues) > 1):
        for obj in whereValues:
            spliterIndx = obj.find(" = ")
            indexKeyStart = obj.find('"')
            # print(indexKeyStart)
            fieldKey = obj[indexKeyStart+1:spliterIndx-1]
            fieldValue = obj[spliterIndx+3:len(obj)]
            fieldValue = fieldValue.replace("'", "")
            data["where"][fieldKey] = fieldValue
            # print(repr(fieldKey), " ==> ", repr(fieldValue))

    # message = Message(scn=scn, seg_owner=seg_owner,
    #                   table_name=table, sql_redo=sql, operation=operation, data=data, timestamp=int(time.time()))

    message = subcriber.singleUpdate(table, data)
    # print(data)
    return message
    # print(message)
    # --- sending to kafka here ???
    # --- end updateParser Function


def deleteParser(scn, sql, seg_owner, table, operation):
    data = {}
    data["value"] = {}
    data["where"] = {}
    whereIndex = sql.find("where\n")
    whereValueString = sql[whereIndex+8:len(sql)]
    whereValueString = whereValueString.replace(" IS NULL", " = NULL")
    whereValues = whereValueString.split(" and \n    ")
    # For multi set key
    if(len(whereValues) > 1):
        for obj in whereValues:
            spliterIndx = obj.find(" = ")
            indexKeyStart = obj.find('"')
            # print(indexKeyStart)
            fieldKey = obj[indexKeyStart+1:spliterIndx-1]
            fieldValue = obj[spliterIndx+3:len(obj)]
            fieldValue = fieldValue.replace("'", "")
            data["where"][fieldKey] = fieldValue

    # print(sql)
    # print(data["where"])
    message = subcriber.singleDelete(table, data)
    # logWriter(scn, sql, table, cfs)
    return message

tables = ["SAD_CONTAINERS",
          "SAD_SUPPLEMENTARY_UNIT",
          "SAD_TAX",
          "UNCMPTAB",
          "UNCUOTAB",
          "UNRATTAB",
          "UN_ASYBRK_IED",
          "UN_ASYBRK_TRACK",
          "VEHICLE_RECEIPT",
          "SAD_GENERAL_SEGMENT",
          "SAD_ITEM",
          "SAD_VECHICLE"]


def get_tables():
    output = ''
    for table in tables:
        output += "'" + table + "'"
        output += ", "
    return output[:-2]


def Task():

    stmt = ""
    isNotEnd = 0
    kafkaServer = KafkaTopic.Server()

    con = cx_Oracle.connect('lipz/lipz#@10.0.10.4/ASYWDB')
    cur = con.cursor()
    sql = "select scn,row_id,csf,seg_owner,table_name,operation,sql_redo from VIEW_LOGMINER where seg_owner='AWUNADM' and table_name in ({0}) order by commit_timestamp,SCN, ROW_ID, TABLE_NAME, CSF desc".format(
        get_tables())

    for scn, row_id, csf, seg_owner, table_name, operation, sql_redo in cur.execute(sql):
        if(csf != 0):
            stmt = sql_redo
            isNotEnd = 1
            continue
        else:
            if(isNotEnd == 1):
                stmt += sql_redo
                isNotEnd = 0
            else:
                stmt = sql_redo
                isNotEnd = 0

        if operation == 'INSERT':
            print("Insert : ", table_name)
            topicName = "ASYWDB."+seg_owner.upper() + "." + table_name.upper()
            message = insertParser(scn, stmt, seg_owner, table_name, operation)
            kafkaServer.singleMessage(
                topic_name=topicName,
                key=str(scn).encode('utf-8'),
                message=message.json().encode('utf-8')
            )
        elif operation == 'UPDATE':
            print("Update : ", table_name)
            message = updateParser(scn, stmt, seg_owner, table_name, operation)

            producer = KafkaProducer(bootstrap_servers='10.0.10.44:9092')
            topicName = "ASYWDB."+seg_owner.upper() + "." + table_name.upper()
            # topicName = "ASYWDB.AWUNADM.UN_ASYBRK_TRACK"
            # producer.send(
            #     topic=topicName,
            #     key=str(scn).encode('utf-8'),
            #     value=message.json().encode('utf-8')
            # )
            kafkaServer.singleMessage(
                topic_name=topicName,
                key=str(scn).encode('utf-8'),
                message=message.json().encode('utf-8')
            )

        elif operation == 'DELETE':
            print("Delete : ", table_name)
        else:
            print(" -> Operator not found...")


if __name__ == "__main__":
    Task()
    # str = "Hello,123"
    # strSplit = str.split(',')
    # print(len(strSplit))
