import sys
import pandas as pd
from pandas import (DataFrame)
import pandas_datareader as pdr
import datetime
import numpy as np
from pandas_datareader import data, wb
from collections import defaultdict
import pymysql
 
def Print_Market_Profile(height_precision = 1, 
    frequency='m', start_date=None, end_date=None):
    try:	
        db_connection = pymysql.connect(host='localhost',user='root',passwd='',db='crude') 
        cursor = db_connection.cursor()
        stmt = "SHOW TABLES LIKE 'upstox_broker_info'"
        cursor.execute(stmt)
        result = cursor.fetchone()
        if result:
            print ("a53787655 table is exist")
        else:
            print ("a53787655 table is not exist")
            exit
    except pymysql.Error as err:
        print(err)
        exit
    # We will look at stock prices over the past year
    if start_date == None:
        # get a year's worth of data from today
        start_date = datetime.date.today() - datetime.timedelta(days=365.24)
        # set date to first of month
        start_date = start_date.replace(day=1)
    if end_date == None:
        end_date = datetime.date.today() 
    
    datatable_name = 'upstox_broker_info'

    #print ("SELECT * FROM "+datatable_name+" WHERE date BETWEEN '"+str(start_date)+"' AND '"+str(end_date)+"'")
    cursor.execute('SELECT * FROM a53787655 where ts >= NOW() - INTERVAL 3 DAY')
    result = cursor.fetchall()
    upstox_frame = DataFrame(np.array(result).reshape(len(result),3), columns=['id','time', 'Last_price'])
    upstox_frame = upstox_frame.loc[:,'time':'Last_price']
    upstox_frame = upstox_frame.set_index('time', 'Last_price')
    upstox_frame = upstox_frame.astype(float)
    upstox_frame = upstox_frame.resample('5Min').ohlc().ffill()
    upstox_frame.columns = ['open', 'high', 'low', 'close']

    upstox_frame[('high')] = upstox_frame[('high')] * height_precision
    upstox_frame[('low')] = upstox_frame[('low')] * height_precision
    upstox_frame = upstox_frame.round({'low': 0, 'high': 0}) 
    time_groups = upstox_frame.groupby(pd.Grouper(freq=frequency))['close'].mean()
    current_time_group_index=0
       
    from collections import defaultdict
    mp = defaultdict(str)
    char_mark = 64

    # build dictionary with all needed prices
    tot_min_price=min(np.array(upstox_frame['low']))
    tot_max_price=max(np.array(upstox_frame['high']))
    for price in range(int(tot_min_price), int(tot_max_price)):
        mp[price]+=('\t')

    # add max price as it will be ignored in for range loop above
    mp[tot_max_price] = '\t' + str(time_groups.index[current_time_group_index])[5:7] + '/' + str(time_groups.index[current_time_group_index])[3:4]
             
    for x in range(0, len(upstox_frame)):
        if upstox_frame.index[x] > time_groups.index[current_time_group_index]:
            # new time period
            char_mark=64
            # buffer and tab all entries
            buffer_max = max([len(v) for k,v in mp.items()])
            current_time_group_index += 1
            for k,v in mp.items():
                mp[k] += (chr(32) * (buffer_max - len(mp[k]))) + '\t'
            mp[tot_max_price] += str(time_groups.index[current_time_group_index])[5:7] + '/' + str(time_groups.index[current_time_group_index])[3:4]
            

        char_mark += 1
        min_price=upstox_frame['low'][x]
        max_price=upstox_frame['high'][x]
        for price in range(int(min_price), int(max_price)):
            mp[price]+=(chr(char_mark))
 
    sorted_keys = sorted(mp.keys(), reverse=True)
    for x in sorted_keys:
        # buffer each list
        print(str("{0:.2f}".format((x * 1.0) / height_precision)) + ': \t' + ''.join(mp[x]))
 
def main():
    # customize ingestion of agruments to handle
    # frequency: http://nullege.com/codes/search/pandas.TimeGrouper

    if (len(sys.argv[1:]) == 1):
        height_precision = float(sys.argv[1:][0])
        Print_Market_Profile(height_precision)
    elif (len(sys.argv[1:]) == 2):
        height_precision = float(sys.argv[1:][0])
        frequency = sys.argv[1:][1]
        Print_Market_Profile(height_precision, frequency)
    else:
        Print_Market_Profile()

if __name__ == "__main__":
    main()