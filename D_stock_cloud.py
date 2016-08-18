#!/usr/bin/env python
# coding=utf-8
import dispy
import dispy.httpd
import logging
import tushare as ts

all_stock_basics=ts.get_stock_basics()
"""
Get all stock code.
"""
def Get_all_code():
    code=all_stock_basics['name'].index
    return code.tolist()


"""
Get ticks for specify code
"""
def Get_stock_ticks(code, time_to_market):
    import tushare as ts
    import pandas as pd
    import logging
    import datetime as dt
    import os
    import socket
    import pydoop.hdfs as hdfs
    import shutil

    if time_to_market !=0:
	logger = logging.getLogger("D_stock")
	logger_handler=logging.FileHandler("/tmp/D_stock.log")
	logger_handler.setFormatter(logging.Formatter("%(asctime)s -- %(message)s"))
	logger_handler.setLevel(logging.DEBUG)
	logger.setLevel(logging.DEBUG)
	logger.addHandler(logger_handler)
        logger.info(">"*15+code+">"*15)

        all_days=pd.date_range(start=str(time_to_market),end=dt.date.today(),freq="B")
        all_days=[x.date() for x in all_days]
        for day in all_days[::-1]:
            logger.info("Saving "+code+"@"+str(day)+"...")
            while True:
                try:
                    df=ts.get_tick_data(code,date=day)
                except Exception as e:
                    print e
                    continue
                break

            if df.index.size >3:
                dir_name="/tmp/ticks/"+str(code)
                if not os.path.exists(dir_name):
                    os.makedirs(dir_name)

                file_name=dir_name+"/"+str(day)+".csv"
                df.to_csv(file_name)
        """
        Write to HDFS        
        """
        if os.path.exists(dir_name):
            s=hdfs.hdfs(host="spark-1",port=9000)
            if not s.exists("ticks"):
                s.create_directory("ticks")
            hdfs.put(dir_name,"./ticks/")
            shutil.rmtree(dir_name)

        logger.info("<"*15+code+"<"*15)
    return (socket.gethostname(),code)
def compute(code):
    import time, socket
    time.sleep(10)
    host=socket.gethostname()
    return (host,code)

if __name__=="__main__":
    
    logger = logging.getLogger("D_stock")
    logger_handler=logging.FileHandler("/tmp/D_stock.log")
    logger_handler.setFormatter(logging.Formatter("%(asctime)s -- %(message)s"))
    logger_handler.setLevel(logging.DEBUG)
    logger.setLevel(logging.DEBUG)
    logger.addHandler(logger_handler)

    cluster=dispy.JobCluster(Get_stock_ticks,nodes=[('172.16.0.133',51348),('172.16.0.134',51348),('172.16.0.131',51348),('172.16.0.132',51348)])
    http_server=dispy.httpd.DispyHTTPServer(cluster)
    jobs=[]
    code_lists=Get_all_code()
    step=0
    for code in code_lists:
    	time_to_market=all_stock_basics.loc[code, 'timeToMarket']
    	if time_to_market !=0:
        	job=cluster.submit(code, time_to_market)
                job.id=int(code)
        	logger.info(str(code))
        	jobs.append(job)
                step+=1
                if step == 8:
                    break

    for job in jobs:
        host,n=job()
        print type(job)
        logger.info('%s executed job %s at %s with stock-code %s'%(host,job.id,job.start_time,n))
    cluster.wait()
    http_server.shutdown()
    cluster.print_status()

