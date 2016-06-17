import tushare as ts
import MySQLdb
import datetime,time
import sys
reload(sys)
sys.setdefaultencoding("utf8")

holidays = ('2016-06-09','2016-06-10','2016-09-15','2016-09-16','2016-10-01','2016-10-02',
            '2016-10-03','2016-10-04','2016-10-05','2016-10-06','2016-10-07')

def get_stock_code(cursor):
    get_code_sql = "select SYMBOL from SH_SZ_CODE;";
    cursor.execute(get_code_sql)
    results = cursor.fetchall()
    return results

def is_trade_day():
    day_of_week = time.strftime("%w",time.localtime())
    print(day_of_week)
    if day_of_week == 0 or day_of_week == 6:
        return False
    current_day = time.strftime('%Y-%m-%d',time.localtime(time.time()))
    for not_work_day in holidays:
        if(current_day == not_work_day):
            return False
    return True

def is_trade_time():
    if(False == is_trade_day()):
        return False
    current_time = int(time.strftime('%H%M%S',time.localtime(time.time())))
    if((current_time > 92500 and current_time < 113000) or (current_time > 130000 and current_time < 150000)):
        return True
    return False

def get_yesterday():
    return time.strftime('%Y-%m-%d',time.localtime(time.time() - 24 * 60 * 60))

def get_yesterday_k_data_by_code(stock_code):
    str_yesterday = get_yesterday()
    return ts.get_hist_data(stock_code,start=str_yesterday,end=str_yesterday)

def insert_k_data(stock_code,result,cursor,db):
    for stock_index in range(0,len(result.index)):
                try:
                    sql = "INSERT INTO algo_get_hist_data(code,date,open,high,close,low) VALUES"
                    #print("stock_index=" + str(stock_index))
                    sql += "("
                    sql += "'" + str(stock_code) + "'" + ','
                    sql += "'" + str(result.index[stock_index]) + "'" + ','
                    sql += str(result.iloc[stock_index,0]) + ','
                    sql += str(result.iloc[stock_index,1]) + ','
                    sql += str(result.iloc[stock_index,2]) + ','
                    sql += str(result.iloc[stock_index,3])
                    sql += ");"
                    cursor.execute(sql)
                    db.commit()
                except Exception, e:
                    print("sql=" + sql)
                    print(e)
    
    