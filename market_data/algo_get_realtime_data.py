import tushare as ts
import MySQLdb
import datetime,time
import algo_stock_common
import sys
from algo_stock_common import is_trade_time
reload(sys)
sys.setdefaultencoding("utf8")

indexs = ('sh000001','sh399001','sh399006')

def get_index_data(starttime):
    sql = "REPLACE INTO algo_today_stock(CODE,changepercent,trade,OPEN,high,low,settlement,TYPE,time,volume,amount) VALUES"
    try:
        result = ts.get_index()
    except Exception, e:
        print(e)
        return
    for stock_index in range(0,len(result.index)):
    #for stock_index in range(0,1):
        code = str(result.iloc[stock_index,0])
        if code == '000001':
            code = 'sh000001'
        if code == '399001':
            code = 'sh399001'
        if code == '399006':
            code = 'sh399006'
        sql += "("
        sql += "'" + code + "'" + ','
        sql += str(result.iloc[stock_index,2]) + ','
        sql += str(result.iloc[stock_index,5]) + ','
        sql += str(result.iloc[stock_index,3]) + ','
        sql += str(result.iloc[stock_index,6]) + ','
        sql += str(result.iloc[stock_index,7]) + ','
        sql += str(result.iloc[stock_index,4]) + ','
        sql += str(0) + ','
        sql += str(starttime) + ','
        sql += str(result.iloc[stock_index,8]) + ','
        sql += str(result.iloc[stock_index,9])
        sql += ")" + ','
    
    sql = sql[:-1]
    sql + ';';
    print("\nafter get data,time=" + time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(starttime)))
    try:
        cursor.execute(sql)
        db.commit()
    except Exception, e:
        db.rollback()
        print(sql)
        print(e)
    
global db
global cursor
global starttime
starttime = int(time.time())

db = MySQLdb.connect("222.73.34.92","root","dataservice2015","test",charset="utf8")
cursor = db.cursor()

while(True):
    if(False == is_trade_time()):
        db.ping()
        time.sleep(50)
    else:
        starttime = int(time.time())
        #continue
    try:
        result = ts.get_today_all()
        stock_realtime = []
        result_index_len = len(result.index)
        current_index = 0;
        current_num = 0
        while(current_index < result_index_len):
            sql = "REPLACE INTO algo_today_stock(CODE,changepercent,trade,OPEN,high,low,settlement,TYPE,time,volume,amount) VALUES"
            print('current_index=%d,result_index_len=%d' % (current_index,result_index_len))
            current_num = 0
            for stock_index in range(current_index,result_index_len):
            #for stock_index in range(0,1):
                sql += "("
                sql += "'" + str(result.iloc[stock_index,0]) + "'" + ','
                sql += str(result.iloc[stock_index,2]) + ','
                sql += str(result.iloc[stock_index,3]) + ','
                sql += str(result.iloc[stock_index,4]) + ','
                sql += str(result.iloc[stock_index,5]) + ','
                sql += str(result.iloc[stock_index,6]) + ','
                sql += str(result.iloc[stock_index,7]) + ','
                sql += str(0) + ','
                sql += str(starttime) + ','
                sql += str(result.iloc[stock_index,8]) + ','
                sql += str(result.iloc[stock_index,10])
                sql += ")" + ','
                current_num += 1
                if current_num > 100:
                    break;
            
            sql = sql[:-1]
            sql + ';';
            print("\nafter get data,time=" + time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(starttime)))
            cursor.execute(sql)
            db.commit()
            print("after insert data,time=" + time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time())))
            current_index += current_num
        get_index_data(starttime)
        sql = "SELECT MAX(TIME) FROM algo_today_stock;"
        cursor.execute(sql)
        db.commit()
        max_time_result = cursor.fetchall()
        for row in max_time_result:
            max_time = row[0]
            print("max_time=" + str(max_time))
            sql = "DELETE FROM algo_today_stock WHERE TIME < "
            sql += str(max_time)
            sql += ";"
            print("delete sql=%s" % sql)
            cursor.execute(sql)
            db.commit()
        sql = "SELECT MAX(TIME) FROM algo_limit_data;"
        cursor.execute(sql)
        db.commit()
        max_limit_data_time_results = cursor.fetchall()
        if len(max_limit_data_time_results) > 0:
            max_limit_data_time = max_limit_data_time_results[0][0]
            max_limit_data_time = max_limit_data_time - 10 * 60 * 60
            sql = "DELETE FROM algo_limit_data WHERE TIME < "
            sql += str(max_limit_data_time)
            sql += ";"
            print("delete limit data sql = %s" % sql)
            cursor.execute(sql)
            db.commit()
    except Exception, e:
        db.close()
        db = MySQLdb.connect("222.73.34.92","root","dataservice2015","test",charset="utf8")
        cursor = db.cursor()
        print(sql)
        print(e)
        continue
    
db.close()