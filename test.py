import tushare as ts
import pandas as pd
import datetime as dt
import os
import multiprocessing as mp

all_stock_basics=ts.get_stock_basics()
"""
Get all stock code.
"""
def Get_all_code():
    code=all_stock_basics['name'].index
    return code.tolist()

"""
Get basic data from timeToMarket to today.
"""
def Get_stock_basic(code):
    time_to_market=all_stock_basics.loc[code, 'timeToMarket']
    if time_to_market !=0:
        print code, time_to_market
        time_str=str(time_to_market)
        time_str=time_str[:4]+'-'+time_str[4:-2]+'-'+time_str[-2:]
        df=ts.get_h_data(code, time_str)
        if df is not None:
            file_name="./stock_basics/"+str(code)+".csv"
            df.to_csv(file_name)

"""
Get ticks for specify code
"""
def Get_stock_ticks(code):
    time_to_market=all_stock_basics.loc[code, 'timeToMarket']
    if time_to_market !=0:
        print ">"*15+code+">"*15
        all_days=pd.date_range(start=str(time_to_market),end=dt.date.today(),freq="B")
        all_days=[x.date() for x in all_days]
        for day in all_days[::-1]:
            print "Saving "+code+"@"+str(day)+"..."
            while True:
                try:
                    df=ts.get_tick_data(code,date=day)
                except Exception as e:
                    print e
                    continue
                break

            if df.index.size >3:
                dir_name="./ticks/"+str(code)
                if not os.path.exists(dir_name):
                    os.makedirs(dir_name)

                file_name=dir_name+"/"+str(day)+".csv"
                df.to_csv(file_name)
        print "<"*15+code+"<"*15

if __name__ == "__main__":
    code_lists=Get_all_code()
#    for code in code_lists:
#        Get_stock_basic(code)
    pool=mp.Pool(processes=mp.cpu_count())
    pool.map(Get_stock_ticks, code_lists)
    pool.close()
    pool.join()

#    for code in code_lists:
#        Get_stock_ticks(code)
