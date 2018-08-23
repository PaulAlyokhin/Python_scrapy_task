import sys
import pandas as pd
import pandas_datareader as pdr
import datetime
import numpy as np
from pandas_datareader import data, wb
from collections import defaultdict
import mysql.connector
from mysql.connector import errorcode
 
def Print_Market_Profile(symbol, height_precision = 1, 
    frequency='m', start_date=None, end_date=None):
    try:	
        config = {
        'user': 'root',
        'password': 'admin',
        'host': '127.0.0.1',
        'database': 'tick_db',
        'raise_on_warnings': True,
        }
        cnx = mysql.connector.connect(**config)
        cursor = cnx.cursor()
        stmt = "SHOW TABLES LIKE 'upstox_broker_info'"
        cursor.execute(stmt)
        result = cursor.fetchone()
        if result:
            print ("upstox_broker_info table is exist")
        else:
            print ("upstox_broker_info table is not exist")
            exit
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with your user name or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")
        else:
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

    fin_prod_data = pdr.get_data_yahoo('GLD', '2017-07-20', '2018-08-20')
    fin_prod_data[('High')] = fin_prod_data[('High')] * height_precision
    fin_prod_data[('Low')] = fin_prod_data[('Low')] * height_precision
    fin_prod_data = fin_prod_data.round({'Low': 0, 'High': 0})  
     
    time_groups = fin_prod_data.groupby(pd.Grouper(freq='m'))['Adj Close'].mean()
    current_time_group_index=0
       
    from collections import defaultdict
    mp = defaultdict(str)
    char_mark = 64

    # build dictionary with all needed prices
    tot_min_price=min(np.array(fin_prod_data['Low']))
    tot_max_price=max(np.array(fin_prod_data['High']))
    for price in range(int(tot_min_price), int(tot_max_price)):
        mp[price]+=('\t')

    # add max price as it will be ignored in for range loop above
    mp[tot_max_price] = '\t' + str(time_groups.index[current_time_group_index])[5:7] + '/' + str(time_groups.index[current_time_group_index])[3:4]
             
    for x in range(0, len(fin_prod_data)):
        if fin_prod_data.index[x] > time_groups.index[current_time_group_index]:
            # new time period
            char_mark=64
            # buffer and tab all entries
            buffer_max = max([len(v) for k,v in mp.items()])
            current_time_group_index += 1
            for k,v in mp.items():
                mp[k] += (chr(32) * (buffer_max - len(mp[k]))) + '\t'
            mp[tot_max_price] += str(time_groups.index[current_time_group_index])[5:7] + '/' + str(time_groups.index[current_time_group_index])[3:4]
            

        char_mark += 1
        min_price=fin_prod_data['Low'][x]
        max_price=fin_prod_data['High'][x]
        for price in range(int(min_price), int(max_price)):
            mp[price]+=str(fin_prod_data['Volume'][x])+','

    sorted_keys = sorted(mp.keys(), reverse=True)
    for x in sorted_keys:
        # buffer each list
        print(str("{0:.2f}".format((x * 1.0) / height_precision)) + ': \t' + ''.join(mp[x]))
 
def main():
    # customize ingestion of agruments to handle
    # frequency: http://nullege.com/codes/search/pandas.TimeGrouper

    if (len(sys.argv[1:]) == 1):
        symbol = sys.argv[1:][0]
        Print_Market_Profile(symbol)
    elif (len(sys.argv[1:]) == 2):
        symbol = sys.argv[1:][0]
        height_precision = float(sys.argv[1:][1])
        Print_Market_Profile(symbol, height_precision)
    elif (len(sys.argv[1:]) == 3):
        symbol = sys.argv[1:][0]
        height_precision = float(sys.argv[1:][1])
        frequency = sys.argv[1:][2]
        Print_Market_Profile(symbol, height_precision, frequency)

if __name__ == "__main__":
    main()