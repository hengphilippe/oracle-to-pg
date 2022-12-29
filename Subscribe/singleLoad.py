import json
from os import PRIO_USER
from tabnanny import check
from traceback import print_tb
from numpy import record, str_
import psycopg2
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
