    
from dailyprice import DailyPrice


if __name__=="__main__":
    
    
    path=r'D:\pyhton\stock_screener\app.db' # path for sqlite database
    # connection to sqlite database
    conn=DailyPrice.dataBaseSqliteConn(path)
    c=DailyPrice.dataBaseSqliteCursor(conn)
    
    # # connection to sqlite database
    conn1=DailyPrice.dataBasePgsConn()
    c1=DailyPrice.dataBasePgsCursor(conn1)
    

    rbi_ref,cur_dict=DailyPrice.rbi_dict(c)
    DailyPrice.rbiSqlite(c,conn,rbi_ref,cur_dict)
    print("rbi sqlite done")
    DailyPrice.rbiPgs(c1,conn1,rbi_ref,cur_dict)
    print("rbi postgres done")

    cu_url = 'https://www.westmetall.com/en/markdaten.php?action=table&field=LME_Cu_cash'
    cu_df = DailyPrice.dfFromURL(cu_url)
    DailyPrice.metal_sqlite(cu_df,c,conn)
    print("copper sqqlite done")
    DailyPrice.cu_pgs(cu_df,c1,conn1)
    print("copper pgs done")
    
    folder_location = r'C:\Users\Admin\Desktop\Python\Pandas\database\stock_screener'
    dateFormated=DailyPrice.bavcopyDate()
    url=DailyPrice.bhavcopyUrl(dateFormated) 
    downloadDf=DailyPrice.bhavcopy(url,folder_location) # downloaad file from nse website
   
    stockdfsqlite=DailyPrice.stockData( downloadDf,c) #cleaning the file for sqqlite
    DailyPrice.stock_sqlite(stockdfsqlite,c,conn) #final writing to the database
    print("stock sqlite done")

    stockdfPgs=DailyPrice.stockData( downloadDf,c1) #cleaning the file for pgs
    DailyPrice.stock_pgs(stockdfPgs,c1,conn1) #final writing to the database
    print("stock postgres done")

    indexdata=DailyPrice.indexDf()
    

    mainIndex,mainIndexDict=DailyPrice.broaderIndex(indexdata,c)
    sectorIndex=DailyPrice.sectIndex(indexdata,c)
    DailyPrice.index_sqlite(mainIndex,mainIndexDict,c,conn)
    print("index sqqlite done")
    DailyPrice.sector_sqlite(sectorIndex,c,conn)
    print("sector sqqlite done")
    

    mainIndexpgs,mainIndexDictpgs=DailyPrice.broaderIndex(indexdata,c1)
    sectorIndexpgs=DailyPrice.sectIndex(indexdata,c1)
    DailyPrice.index_pgs(mainIndexpgs,mainIndexDictpgs,c1,conn1)
    print("index pgs done")
    DailyPrice.sector_pgs(sectorIndexpgs,c1,conn1)
    print("sector pgs done")








