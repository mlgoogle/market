import tushare as ts
import MySQLdb
import datetime
import time
import sys
reload(sys)
sys.setdefaultencoding("utf8")

db = MySQLdb.connect("222.73.57.14","root","dataservice2015","test",charset="utf8")
cursor = db.cursor()

while(True):
    sql = "INSERT INTO algo_get_stock_basics(code,name,totalAssets,bvps,pb,mtm,time) VALUES"
    try:
        result = ts.get_stock_basics()
    except Exception, e:
        print(e)
        continue
    last_element = len(result.index)
    stock_realtime = []
    starttime = int(time.time())
    for stock_index in range(0,len(result.index)):
    #for stock_index in range(0,1):
        sql += "("
        sql += "'" + str(result.index[stock_index]) + "'" + ','
        sql += "'" + str(result.iloc[stock_index,0]) + "'" + ','
        sql += str(result.iloc[stock_index,5]) + ','
        sql += str(result.iloc[stock_index,12]) + ','
        sql += str(result.iloc[stock_index,13]) + ','
        sql += str(result.iloc[stock_index,13] * result.iloc[stock_index,12] * result.iloc[stock_index,5]) + ','
        sql += str(starttime)
        sql += ")" + ','
    
    sql = sql[:-1]
    sql + ';';
    print("\nafter get data,time=" + time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(starttime)))
    try:
        cursor.execute(sql)
        db.commit()
        print("after insert data,time=" + time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time())))
        time.sleep(60 * 60)
    except Exception, e:
        db.rollback()
        print(sql)
        print(e)

    

db.close()