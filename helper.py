import datetime 
import sqlite3
import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2.extensions import register_adapter, AsIs
import pandas as pd

def data_base():
    conn=sqlite3.connect(r'D:\pyhton\stock_screener\app.db')
    conn.row_factory=sqlite3.Row # this help to retrive the iteration more like a dictionery
    c=conn.cursor()
    return conn,c

def data_base_pgs():
    while True:   
        try:
            conn=psycopg2.connect(host='localhost',database='market',user='postgres',password='1234',
                                        cursor_factory=RealDictCursor)
            c=conn.cursor()
            print("Database connection was sucessful")
            break
        except Exception as error:
            print("Not Connected")
            print("the error was",error)
            datetime.time.sleep(2)
    return conn,c
