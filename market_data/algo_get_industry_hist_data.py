import tushare as ts
import MySQLdb
import datetime
import time
import algo_stock_common
import sys
reload(sys)
sys.setdefaultencoding("utf8")

db = MySQLdb.connect("222.73.34.92","root","dataservice2015","test",charset="utf8")
cursor = db.cursor()
stock_codes = algo_stock_common.get_stock_code(cursor)

def insert_stock_data_by_code_and_type(stock_code,k_type):
    result = ts.get_hist_data(stock_code, ktype=k_type)
    for stock_index in range(0,len(result.index)):
        try:
            sql = "REPLACE INTO algo_get_industry_hist_data(code,date,open,high,close,low,volume,p_change,type) VALUES"
            #print("stock_index=" + str(stock_index))
            sql += "("
            sql += "'" + str(stock_code) + "'" + ','
            sql += "'" + str(result.index[stock_index]) + "'" + ','
            sql += str(result.iloc[stock_index,0]) + ','
            sql += str(result.iloc[stock_index,1]) + ','
            sql += str(result.iloc[stock_index,2]) + ','
            sql += str(result.iloc[stock_index,3]) + ','
            sql += str(result.iloc[stock_index,4]) + ','
            sql += str(result.iloc[stock_index,6]) + ','
            sql += "'" + str(k_type) + "'"
            sql += ");"
            print("sql=",sql)
            starttime = int(time.time())
            #print("\nafter get data,time=" + time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(starttime)))
            stock_realtime = []
            starttime = int(time.time())
            cursor.execute(sql)
            db.commit()
            #time.sleep(1)
            break;
            #print("after insert data,time=" + time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time())))
        except Exception, e:
            print("sql=" + sql)
            print(e)
    

while(True):
    
    for stock_code in stock_codes:
        try:
            insert_stock_data_by_code_and_type(stock_code[0], 'W')
            insert_stock_data_by_code_and_type(stock_code[0], 'M')
        except Exception, e:
                db.rollback()
                print("insert_stock_data_by_code_and_type failed" + str(stock_code[0]))
                print(e)
    time.sleep(24*60*60)
    db.close()
    db = MySQLdb.connect("222.73.34.92","root","dataservice2015","test",charset="utf8")
    cursor = db.cursor()
db.close()