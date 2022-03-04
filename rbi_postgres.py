import numpy as np
import psycopg2
from psycopg2.extensions import register_adapter, AsIs
import datetime
import nsepy
from helper import data_base_pgs


c=data_base_pgs()[0]
conn=data_base_pgs()[1]


# getting RBI reference using NSEpy module. details can be found in nsepy_testing file in stock
rbi_ref = nsepy.get_rbi_ref_history(datetime.date.today(), datetime.date.today())
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
        register_adapter(np.int64, psycopg2._psycopg.AsIs)
        date=str(datetime.datetime.today().date())
        val=[v for v in cur_dict.values()]
        c.execute("""INSERT INTO rbi_exchange(date,cur_id,rate ) VALUES ( %s,%s,%s)""",(date,val[i],rbi_ref.iloc[0][i]))
conn.commit()  
