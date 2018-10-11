import datetime
import tushare as ts
import pymysql
import csv
import pandas as pd
#state_dt,stock_code,open,close,high,low,vol,amount,pre_close,amt_change,pct_change
root_path = "E:/ubs/stock_dfs_sample/"


def get_date_format():
    # start_pos = -1259
    start_pos = -100
    df1 = pd.DataFrame()
    A = pd.read_csv('AAPL.csv')
    df1['Date'] = A.iloc[start_pos:, 0]
    df1.index = df1.iloc[start_pos:, 0]
    df1.index = pd.to_datetime(df1.index, format = '%Y-%m-%d')
    # df1.pop('Date')
    return df1['Date'].values.tolist()


def get_stock_from_local(state_dt,stock_code):
    file_path =root_path + stock_code + ".csv"
    with open(file_path) as f:
        f_csv = csv.reader(f)
        headers = next(f_csv)
        param_list = []
        global previous_close
        param_list.append(state_dt)
        param_list.append(stock_code)
        for row in f_csv:
            if state_dt == row[0]:
                param_list.append(row[3])
                param_list.append(row[4])
                param_list.append(row[1])
                param_list.append(row[2])
                param_list.append(row[5])
                param_list.append(-1)
                param_list.append(previous_close)
                param_list.append(float(row[4])-float(row[3]))
                param_list.append((float(row[4])-float(row[3]))/(float(previous_close)))
            previous_close = row[4]
        return param_list


if __name__ == '__main__':

    # 建立数据库连接,剔除已入库的部分
    db = pymysql.connect(host='127.0.0.1', user='root', passwd='081337', db='stock', charset='utf8')
    cursor = db.cursor()
    # 设定需要获取数据的股票池
    stock_pool = ['A','AA']
    total = len(stock_pool)
    # 获取日期序列，从2018.5到2018.9.24
    date_list = get_date_format()

    # 循环获取单个股票的日线行情
    for i in range(len(stock_pool)):
        for date in date_list:
            resu0 = get_stock_from_local(date, stock_pool[i])
            resu = []
            for k in range(len(resu0)):
                if str(resu0[k]) == 'nan':
                    resu.append(-1)
                else:
                    resu.append(resu0[k])
            state_dt = date
            print(resu)
            try:
                sql_insert ='''INSERT INTO stock_all(state_dt,stock_code,open,close,high,low,vol,amount,pre_close,amt_change,pct_change) 
                            VALUES ('%s', '%s', '%.2f', '%.2f','%.2f','%.2f','%i','%.2f','%.2f','%.2f','%.2f')''' \
                             % (state_dt,str(resu[1]),float(resu[2]),float(resu[3]),float(resu[4]),float(resu[5]),float(resu[6]),float(resu[7]),float(resu[8]),float(resu[9]),float(resu[10]))
                cursor.execute(sql_insert)
                db.commit()
            except Exception as err:
                print("Error:"+str(err))
                continue
        print(i)
    cursor.close()
    db.close()
    print('All Finished!')
