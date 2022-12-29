import sys
import json
import psycopg2
from datetime import datetime
from psycopg2.extras import Json
from psycopg2.extensions import AsIs
from psycopg2 import sql
from datetime import datetime

class Subcriber():
    def __init__(self) -> None:
        # forming connection
        self.conn = psycopg2.connect(
            database="sale_entry",
            user='postgres',
            password='[12]root',
            host='10.0.10.41',
            port='5432'
        )

        self.conn.autocommit = True
        # creating a cursor
        self.cursor = self.conn.cursor()

    def insert(self, table, data):
        table = table.split(".")[1]
        datas = json.loads(data)

        # print(type(datas))
        # get the column names
        schema = datas[0]["data"]
        columns = list(schema.keys())
        columns = ['"{}"'.format(elem) for elem in columns]
        if(len(data) > 0):
            # task batch insert perfrom
            # print(columns)
            values_str = ""
            # i = 0
            for records in datas:
                # if(i < 2):
                val_list = []
                values_str = ""
                values = list(records["data"].values())
                for x in values:
                    if type(x) == str:
                        val_list.append("\'"+x+"\'")
                    elif x == None:
                        val_list.append('null')
                    else:
                        val_list.append(x)
                # # print("Values : ", values)
                values_str += "(" + ', '.join(map(str, val_list)) + "),\n"
                values_str = values_str[:-2]
                sql_string = "INSERT INTO %s (%s)\nVALUES %s" % (
                    '"{}"'.format(table.upper()),
                    ', '.join(columns),
                    values_str
                )
                # print(sql_string)

                try:
                    self.cursor.execute(sql_string)
                except (Exception, psycopg2.DatabaseError) as err:
                    print(err)
                # finally:
                #     if self.conn is not None:
                #         self.conn.close()
                #     i = i+1
                # else:
                #     break
            print("Done")
        else:
            print("No record send to supscriber")

    def insert_v2(self, table, datas, start_time):
        print("Starting insert to postgres")
        t1 = datetime.now()
        x = t1-start_time
        # print(f"dump duration was : {x}")

        table = table.split(".")[1]
        # datas = json.loads(data)

        firstMessage = json.loads(datas[0])
        # get the column names
        columns = list(firstMessage["data"].keys())

       

        # get the column names
        # schema = datas[0][0]["data"]
        # columns = ['"{}"'.format(elem) for elem in columns]
        # columns = list(schema.keys())
        # columns.append("START_BATCH")
        # columns.append("END_BATCH")

        # Values
        data = []
        # print(datas)
        
        for records in datas:
            records = json.loads(records)
            # print(records['data'])
            for key,val in records['data'].items() : 
                # print(str(val))
                # found = re.match(pattern,str(val)) 
                if "b'\\x00" in str(val) : 
                # if found : 
                    records['data'][key] = None
            # print(records['data'])
            
            # empty_dict = {}
            # for val in records["data"].values() : 
            #     # data.append(val)
            #     if re.match(pattern, str(val)) :
            #         empty_dict.append(None)
            #     else :  
            #         empty_dict.append(val)
            data.append(records["data"].values())
     
        rows = [tuple(row) for row in data]
        chunks = self.chunker(rows, 25000)
        # chunks = self.chunker(list(data), 25000)

        for chunk in chunks:
            self._insert_(table.upper(), len(columns), chunk)
        t2 = datetime.now()
        x = t2 - t1
        print(f"Finish Postgres batch was : {x}")

    def chunker(self, lst, n):
        chunks = [lst[i * n:(i + 1) * n]
                  for i in range((len(lst) + n - 1) // n)]
        return chunks

    # 8:45Mins
    def _insert_many(self, table, field_count, rows: list):
        print(field_count)
        str_fieds = "("
        i = 0
        while(i < field_count):
            str_fieds += '%s,'
            i = i+1
        str_fieds = str_fieds[:-1]
        str_fieds += ")"
        inputs = [[row[0], row[1], row[2], row[3], row[4], row[5], row[6],
                   row[7], row[8], row[9], row[10], row[11], row[12], row[13], row[14], row[15]] for row in rows]
        try:
            self.cursor.executemany(
                "INSERT INTO {0} VALUES {1}".format('"' + table+'"', str_fieds), inputs)
        except (Exception, psycopg2.DatabaseError) as err:
            print(err)

    # Well Lah 17sec :D
    def _insert_(self, table, field_count, rows: list):
        # print(field_count)
        str_fieds = "("
        i = 0
        while(i < field_count):
            str_fieds += '%s,'
            i = i+1
        str_fieds = str_fieds[:-1]
        str_fieds += ")"

        # print(rows)
  
        args_str = ','.join(self.cursor.mogrify(
            str_fieds, rec).decode('utf8') for rec in rows)
        # pprint.pprint(args_str)

        # start_reg = list(self.find_all(args_str, "b''\\x"))
        # end_reg = list(self.find_all(args_str, "''',"))
        # new_str = ""
        # replace_str = []
        # for i in range(len(start_reg)) :
        #     print(start_reg[0]) 
        #     keystr = str[start_reg[i]:end_reg[i]+2]
        #     print(keystr)
        #     replace_str.append(keystr)

        # for x in replace_str : 
        #     if x in str :
        #         new_str = str.replace(x,'NULL')
        
        try:
            self.cursor.execute(
                "INSERT INTO {0} VALUES ".format(table) + args_str)
        except (Exception, psycopg2.DatabaseError) as err:
            self.print_psycopg2_exception(err, None)
            print(err)

    def singleInsert(self, table, message) :
        # print(message)
        table = table.lower()
        insert_statement = "insert into " + table + " values ("

        # message = {key: val.replace("'", "") for key, val in message.items()}
        # columns = message.keys()
        # values = [message[column] for column in columns]
        values = ",".join(message.values())
        insert_statement += values
        insert_statement += ")"
        insert_statement = insert_statement.replace('EMPTY_CLOB()', 'NULL')
        insert_statement = insert_statement.replace('EMPTY_BLOB()', 'NULL')
        # print(insert_statement)
        status = 0

        # insert_statement = "insert into {0} (%s) values %s"

        # query = sql.SQL(insert_statement).format(
        #     sql.Identifier(table))
        # try:
        #     # self.cursor.execute(
        #     #     query,
        #     #     (AsIs(','.join(columns)),
        #     #      tuple(values))
        #     # )
        #     self.cursor.execute(insert_statement)
        #     status = 1
        # except Exception as err:
        #     self.print_psycopg2_exception(err,insert_statement)
        #     status = 0
        # finally:
        #     return status
        return insert_statement


    def singleUpdate(self, table, message) :
        
        setValues = message["value"]
        whereConditions = message["where"]

        ''' get SET fields and values '''
        # setValues = {key: val.replace("'", "")
        #              for key, val in setValues.items()}
        # columns = setValues.keys()
        # values = [setValues[column] for column in columns]

        ''' get WHERE fields and values '''
        # whereConditions = {key: val.replace("'", "")
        #                    for key, val in whereConditions.items()}
        # whereColumns = whereConditions.keys()
        # whereValues = [setValues[whereColumn] for whereColumn in whereColumns]

        update_statement = "update "+table.lower() + " set "

        for key, val in setValues.items():
            update_statement += (key.lower() + " = '" + val+"',")

        update_statement = update_statement[:-1]
        update_statement += " where "

        for key, val in whereConditions.items():
            update_statement += (key.lower() + " = '" + val + "'")
            update_statement += " and "

        update_statement = update_statement[:-5]
        update_statement = update_statement.replace("'NULL'", "Null")
        # print(repr(update_statement))

        # try:
        #     self.cursor.execute(update_statement)
        #     status = 1
        # except Exception as err:
        #     self.print_psycopg2_exception(err,update_statement)
        #     status = 0
        # finally:
        #     return status

        return update_statement

    def singleDelete(self, table, message) :
       
        whereConditions = message["where"]
        delete_statement = "delete from " + table.lower() + " where "
        for key, val in whereConditions.items():
            delete_statement += (key.lower() + " = '" + val + "'")
            delete_statement += " and "

        delete_statement = delete_statement[:-5]
        delete_statement = delete_statement.replace("'NULL'", "Null")
        # print("\t => Origin : ", message)
        # print("\t -> ", delete_statement)

        # exit()
        # try:
        #     self.cursor.execute(delete_statement)
        #     status = 1
        # except Exception as err:
        #     self.print_psycopg2_exception(err,delete_statement)
        #     status = 0
        # finally:
        #     return status

        return delete_statement

    def find_all(self,a_str, sub):
        start = 0
        while True:
            start = a_str.find(sub, start)
            if start == -1: return
            yield start
            start += len(sub) # use start += 1 to find overlapping matches

    def print_psycopg2_exception(self, err,message):
        now = datetime.now() 
        date_time = now.strftime("%m/%d/%Y, %H:%M:%S")
        error = open("error.log","a+")
        # get details about the exception
        err_type, err_obj, traceback = sys.exc_info()

        # get the line number when exception occured
        line_num = traceback.tb_lineno
        error.write(date_time)
        error.write(message)
        # print the connect() error
        msg_err = f"\npsycopg2 ERROR: {err}, on line number: {line_num}"
        print(msg_err)
        error.write(msg_err)
        msg_err = f"psycopg2 traceback: {traceback},{err_type}"
        print(msg_err)
        error.write(msg_err)
        # psycopg2 extensions.Diagnostics object attribute

        msg_err = f"\nextensions.Diagnostics:, {err.diag}"
        print(msg_err)
        error.write(msg_err)

        # print the pgcode and pgerror exceptions
        msg_err = f"pgerror:, {err.pgerror}"
        print(msg_err)
        error.write(msg_err)

        msg_err = f"pgcode:, {err.pgcode} \n"
        print(msg_err)
        error.write(msg_err)
        error.write("---------------------------------------------- \n")
        error.close()
