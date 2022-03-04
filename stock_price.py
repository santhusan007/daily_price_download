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


    # Code for populating price from NSE website
aaj=dt.date.today().strftime('%d%b%Y').upper()
bhav_url=f'https://archives.nseindia.com/content/historical/EQUITIES/2022/{aaj[2:5]}/cm{aaj}bhav.csv.zip'
print(bhav_url)
def bhavcopy():
    folder_location=r'C:\Users\Admin\Desktop\Python\Pandas\database\stock_screener'
    filename=os.path.join(folder_location,bhav_url.split("/")[-1])
    r=requests.get(bhav_url,stream=True)
    #converting zip content to binary
    z=zipfile.ZipFile(io.BytesIO(r.content))
    #using zip library to read the binary zip adnextract it to destination
    z.extractall(folder_location)
        
    #df = get_price_list(dt=date.today())
    #df=pd.read_csv('cm03JAN2022bhav.csv')
    df=pd.read_csv(filename[:-4])# avoiding the '.zip'(last 4 charcters) extesnion 
    return df
df=bhavcopy()

c.execute (""" SELECT id,symbol,company FROM stocks """ )
rows=c.fetchall()
    #for row in rows:
        #print(row['company'])
        
symbols=[]
stock_dict={}
for row in rows:
        
    symbol=row['symbol']
    symbols.append(symbol)
    stock_dict[symbol]=row['id']
len(stock_dict)
    # Extracting 506 stock details from the whole list of bahv copy
daily_data=df.copy()[(df.copy().SYMBOL.isin (symbols))&((df.copy()['SERIES']=='EQ') | (df.copy()['SERIES']=='BE'))]
# converting the stock dict to data frame
stock_data=pd.DataFrame(stock_dict.items(),columns=['SYMBOL','stk_id'])
# merging to dataframe a
final_df=pd.merge(daily_data,stock_data)
final_df.sort_values('stk_id',inplace=True)
final_df.reset_index(inplace=True)
final_df.drop(columns='index',inplace=True)
final_df['date']=pd.to_datetime(final_df['TIMESTAMP'])
final_df=final_df[['SYMBOL','stk_id','date','OPEN', 'HIGH', 'LOW', 'CLOSE','TOTTRDQTY']]
len(final_df)

for i in range (len(final_df)):
    sqlite3.register_adapter(np.int64,lambda val:float(val))
    sqlite3.register_adapter(np.int32,lambda val:float(val))
    c.execute("INSERT INTO stock_price(stock_id,date,open,high,low,close,volume ) VALUES ( ?,?,?,?,?,?,?)",(final_df['stk_id'].iloc[i],str(final_df['date'].iloc[i]).split(' ')[0],final_df['OPEN'].iloc[i],final_df['HIGH'].iloc[i],final_df['LOW'].iloc[i],final_df['CLOSE'].iloc[i],final_df['TOTTRDQTY'].iloc[i]))
conn.commit()      



