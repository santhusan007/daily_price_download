    
from helper import (dataBasePgsConn,dataBasePgsCursor, metal_sqlite, rbi_dict, 
                cu_pgs,dataBaseSqliteConn,dataBaseSqliteCursor,rbiSqlite,
                dataBasePgsConn,dataBasePgsCursor,rbiPgs,dfFromURL,
                bavcopyDate,bhavcopyUrl,bhavcopy,stock_sqlite,stock_pgs,stockData,
                indexDf,broaderIndex,sectIndex,index_sqlite,sector_sqlite,
                index_pgs,sector_pgs
                )


if __name__=="__main__":
    

    path=r'D:\pyhton\stock_screener\app.db' # path for sqlite database
    # connection to sqlite database
    conn=dataBaseSqliteConn(path)
    c=dataBaseSqliteCursor(conn)
    
    # # connection to sqlite database
    conn1=dataBasePgsConn()
    c1=dataBasePgsCursor(conn1)
    

    rbi_ref,cur_dict=rbi_dict(c)
    rbiSqlite(c,conn,rbi_ref,cur_dict)
    print("rbi sqlite done")
    rbiPgs(c1,conn1,rbi_ref,cur_dict)
    print("rbi postgres done")

    cu_url = 'https://www.westmetall.com/en/markdaten.php?action=table&field=LME_Cu_cash'
    cu_df = dfFromURL(cu_url)
    metal_sqlite(cu_df,c,conn)
    print("copper sqqlite done")
    cu_pgs(cu_df,c1,conn1)
    print("copper pgs done")
    
    folder_location = r'C:\Users\Admin\Desktop\Python\Pandas\database\stock_screener'
    dateFormated=bavcopyDate()
    url=bhavcopyUrl(dateFormated) 
    downloadDf=bhavcopy(url,folder_location) # downloaad file from nse website
   
    stockdfsqlite=stockData( downloadDf,c) #cleaning the file for sqqlite
    stock_sqlite(stockdfsqlite,c,conn) #final writing to the database
    print("stock sqlite done")

    stockdfPgs=stockData( downloadDf,c1) #cleaning the file for pgs
    stock_pgs(stockdfPgs,c1,conn1) #final writing to the database
    print("stock postgres done")

    indexdata=indexDf()
    

    mainIndex,mainIndexDict=broaderIndex(indexdata,c)
    sectorIndex=sectIndex(indexdata,c)
    index_sqlite(mainIndex,mainIndexDict,c,conn)
    print("index sqqlite done")
    sector_sqlite(sectorIndex,c,conn)
    print("sector sqqlite done")
    

    mainIndexpgs,mainIndexDictpgs=broaderIndex(indexdata,c1)
    sectorIndexpgs=sectIndex(indexdata,c1)
    index_pgs(mainIndexpgs,mainIndexDictpgs,c1,conn1)
    print("index pgs done")
    sector_pgs(sectorIndexpgs,c1,conn1)
    print("sector pgs done")








