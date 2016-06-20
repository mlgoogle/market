import tushare as ts
import MySQLdb
import datetime
import time
import algo_stock_common
import sys
reload(sys)
sys.setdefaultencoding("utf8")

def delete_old_data(stock_code,cursor):
    sql = "select date,open from algo_get_hist_data where code = '%s' order by date  desc limit 90;" % stock_code
    print(sql)
    cursor.execute(sql)
    results = cursor.fetchall()
    result_length = len(results)
    print("result_length=%d" % result_length)
    #for r in results:
    #   print("r0=" + r[0] + "r1=" + str(r[1]))
    if (result_length < 60):
        return
    old_time = results[result_length - 1][0]
    sql = "delete from algo_get_hist_data where date < '%s';" % old_time
    print(sql)
    cursor.execute(sql)

def get_yesterday():
    return time.strftime('%Y-%m-%d',time.localtime(time.time() - 24 * 60 * 60))

db = MySQLdb.connect("222.73.34.92","root","dataservice2015","test",charset="utf8")
cursor = db.cursor()

stock_codes = algo_stock_common.get_stock_code(cursor)

while(True):
    
    for stock_code in stock_codes:
        try:
            time.sleep(1)
            print("stock_code=" + stock_code[0])
            str_yesterday = get_yesterday()
            print("str_yesterday=" + str_yesterday)
            result = ts.get_hist_data(stock_code[0],start='2015-05-25',end=str_yesterday)
            hfq_result = ts.get_h_data(stock_code[0],autype='hfq',start='2015-05-25',end=str_yesterday)
            print("get_hist_data of " + stock_code[0])
            #for stock_index in range(0,3):
            for stock_index in range(0,len(result.index)):
                try:
                    date = str(result.index[stock_index])
                    if date == '2015-05-25':
                        continue
                    sql = "REPLACE INTO algo_get_hist_data(code,date,open,high,close,low,volume,price_change,p_change,hfq_open,hfq_high,hfq_close,hfq_low) VALUES"
                    #print("stock_index=" + str(stock_index))
                    sql += "("
                    sql += "'" + str(stock_code[0]) + "'" + ','
                    sql += "'" + str(result.index[stock_index]) + "'" + ','
                    sql += str(result.iloc[stock_index,0]) + ','
                    sql += str(result.iloc[stock_index,1]) + ','
                    sql += str(result.iloc[stock_index,2]) + ','
                    sql += str(result.iloc[stock_index,3]) + ','
                    sql += str(result.iloc[stock_index,4]) + ','
                    sql += str(result.iloc[stock_index,5]) + ','
                    sql += str(result.iloc[stock_index,6]) + ','
                    sql += str(hfq_result.iloc[stock_index,0]) + ','
                    sql += str(hfq_result.iloc[stock_index,1]) + ','
                    sql += str(hfq_result.iloc[stock_index,2]) + ','
                    sql += str(hfq_result.iloc[stock_index,3])
                    sql += ");"
                    #print("sql=end",sql)
                    starttime = int(time.time())
                    #print("\nafter get data,time=" + time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(starttime)))
                    stock_realtime = []
                    starttime = int(time.time())
                    cursor.execute(sql)
                    db.commit()
                    #print("after insert data,time=" + time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time())))
                except Exception, e:
                    print("sql=" + sql)
                    print(e)
            delete_old_data(stock_code, cursor)
            #time.sleep(5 * 60)
        except Exception, e:
                db.rollback()
                print(e)
    time.sleep(60*60)
    db.close()
    db = MySQLdb.connect("222.73.34.92","root","dataservice2015","test",charset="utf8")
    cursor = db.cursor()
    
db.close()