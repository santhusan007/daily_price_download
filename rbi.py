import sqlite3
import numpy as np
import datetime as dt
import nsepy
from helper import data_base

c = data_base()[0]
conn = data_base()[1]


    # getting RBI reference using NSEpy module. details can be found in nsepy_testing file in stock
rbi_ref = nsepy.get_rbi_ref_history(dt.date.today(), dt.date.today())
print(rbi_ref)
c.execute (""" SELECT * FROM currency """ )
rows=c.fetchall()
cur_symbols=[]
cur_dict={}
for row in rows:
        
    cur_symbol=row['cur_symbol']
    cur_symbols.append(cur_symbol)
    cur_dict[cur_symbol]=row['id']

for i in range (len(rbi_ref.T)):
    sqlite3.register_adapter(np.int64,lambda val:float(val))
    sqlite3.register_adapter(np.int32,lambda val:float(val))
    date=str(dt.datetime.today().date())
    val=[v for v in cur_dict.values()]
    c.execute("INSERT INTO rbi_exchange(date,cur_id,rate ) VALUES ( ?,?,?)",(date,val[i],rbi_ref.iloc[0][i]))
conn.commit()  
     