import psycopg2
import sys

listOfSTMT = [
    'INSERT INTO LOGON VALUES (2,2033,CURRENT_DATE)',
    'INSERT INTO LOGON VALUES (3,2033,CURRENT_DATE)',
    'INSERT INTO LOGON VALUES (4,2033,CURRENT_DATE)',
    'UPDATE LOGON SET logon_date=CURRENT_DATE-1 where logon_id=1'
]
con = None

try:
    con = psycopg2.connect(
            database="sale_entry",
            user='postgres',
            password='[12]root',
            host='10.0.10.41',
            port='5432'
        )
    # con.autocommit = True
    #  psycopg2.connect(database='testdb', user='postgres',
    #                 password='s$cret')
    cur = con.cursor()

    for stmt in listOfSTMT : 
        cur.execute(stmt)
    con.commit()

except psycopg2.DatabaseError as e:
    if con:
        con.rollback()

    print(f'Error {e}')
    sys.exit(1)

finally:
    if con:
        con.close()