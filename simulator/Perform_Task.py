from time import sleep
import cx_Oracle
from faker import Faker
import random


def insert_dept(s_id, s_name):
    con = cx_Oracle.connect('soe/soe#@10.0.10.42/DEVDB')
    cur = con.cursor()
    cur.execute("insert into soe.demo (id, name) values (:1, :2)",
                (s_id, s_name))
    cur.close()
    con.commit()
    con.close()


# call the insert_dept function
while(True):
    fake = Faker()
    name = fake.name()
    try:
        insert_dept(random.randint(0, 999), name)
        print(name, " has been added.")
    except Exception as e:
        print(e)
    sleep(5)
