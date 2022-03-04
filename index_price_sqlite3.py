import sqlite3
import pandas as pd
import numpy as np
import datetime as dt
import zipfile
import io
import os
import requests
from datetime import date
from nsepy.history import get_price_list
from helper import data_base

c = data_base()[0]
conn = data_base()[1]

   # INdex PRice from NSE website
def index_price():
    index_aaj = dt.date.today().strftime('%d%m%Y').upper()
    index_url = f'https://archives.nseindia.com/content/indices/ind_close_all_{index_aaj}.csv'
    folder_location = r'C:\Users\Admin\Desktop\Python\Pandas\database\stock_screener'
    filename = os.path.join(folder_location, index_url.split("/")[-1])
    r = requests.get(index_url, stream=True)
    with open(filename, 'wb') as f:
        f.write(r.content)

        index_df = pd.read_csv(filename)
        return index_df
index_df = index_price()

# Making a dict for broader Index
c.execute (""" SELECT * FROM broader_index """)
rows = c.fetchall()
bor_symbols = []
bor_dict = {}
for row in rows:

    bor_symbol = row['indices']
    bor_symbols.append(bor_symbol)
    bor_dict[bor_symbol] = row['id']

df = index_df[index_df['Index Name'].isin(['Nifty 50','NIFTY Midcap 100','NIFTY Smallcap 100'])].reset_index()
df['Index Date'] = pd.to_datetime(df['Index Date'],dayfirst=True)
df.drop(columns='index', inplace=True)
df = round(df,2)
for i in range(len(df)):
    sqlite3.register_adapter(np.int64, lambda val:float(val))
    sqlite3.register_adapter(np.int32, lambda val:float(val))
    broader_id = bor_dict[df['Index Name'].iloc[i]]
    c.execute("INSERT INTO index_price(broader_id,date,open,high,low,close,volume ) VALUES (?,?,?,?,?,?,?)", (broader_id,str(df['Index Date'].iloc[i]).split(' ')[0],df['Open Index Value'].iloc[i],df['High Index Value'].iloc[i],df['Low Index Value'].iloc[i],df['Closing Index Value'].iloc[i],df['Volume'].iloc[i]))
    conn.commit()
c.execute (""" SELECT * FROM sectorial_index """)
rows = c.fetchall()
sect_symbols = []
sect_dict = {}
for row in rows:

    sect_symbol = row['sector']
    sect_symbols.append(sect_symbol)
    sect_dict[sect_symbol] = row['id']

sect_df = index_df[index_df['Index Name'].isin(list(sect_dict.keys())[:-1])].reset_index()
sect_df['Index Date'] = pd.to_datetime(sect_df['Index Date'],dayfirst=True)
sect_df.drop(columns='index', inplace=True)
stock_data = pd.DataFrame(sect_dict.items(),columns=['Index Name','stk_id'])
df = pd.merge(sect_df,stock_data)
df.sort_values('stk_id', inplace=True)
for i in range(len(df)):
    sqlite3.register_adapter(np.int64, lambda val:float(val))
    sqlite3.register_adapter(np.int32, lambda val:float(val))
    c.execute("INSERT INTO index_price(sectorial_id,date,open,high,low,close,volume ) VALUES (?,?,?,?,?,?,?)", (df['stk_id'].iloc[i],str(df['Index Date'].iloc[i]).split(' ')[0],df['Open Index Value'].iloc[i],df['High Index Value'].iloc[i],df['Low Index Value'].iloc[i],df['Closing Index Value'].iloc[i],df['Volume'].iloc[i]))

conn.commit()
