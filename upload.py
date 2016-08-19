#!/usr/bin/env python
# coding=utf-8
import os
#import shutil
import logging
import dispy
import dispy.httpd

logger = logging.getLogger("Upload")
logger_handler=logging.FileHandler("/tmp/Upload.log")
logger_handler.setFormatter(logging.Formatter("%(asctime)s -- %(message)s"))
logger_handler.setLevel(logging.DEBUG)
logger.setLevel(logging.DEBUG)
logger.addHandler(logger_handler)

def _Upload(directory):
    import logging
    import os
    import socket
    logger = logging.getLogger("Upload")
    logger_handler=logging.FileHandler("/tmp/Upload.log")
    logger_handler.setFormatter(logging.Formatter("%(asctime)s -- %(message)s"))
    logger_handler.setLevel(logging.DEBUG)
    logger.setLevel(logging.DEBUG)
    logger.addHandler(logger_handler)
    logger.info(directory)
    command="scp -qr hfli@172.16.0.100:/srv/work/D_stock/ticks/" + directory +" /tmp/"+directory
    
    while True:
        try:
            os.system(command)
        except Exception as e:
            print e
            continue
        break
    logger.info(command)
    command="~/hadoop/bin/hadoop fs -put /tmp/"+directory+" ./ticks/"

    while True:
        try:
            os.system(command)
        except Exception as e:
            print e
            clean_command="~/hadoop/bin/hadoop fs -rm -r ./ticks/"+directory
            os.system(clean_command)
            continue
        break

    logger.info(command)
    command="rm -rf /tmp/"+directory
    os.system(command)
    return (socket.gethostname(),directory)
    
if __name__=="__main__":
    cluster=dispy.JobCluster(_Upload,nodes=[('172.16.0.133',51348),('172.16.0.134',51348),('172.16.0.131',51348),('172.16.0.132',51348)])
    http_server=dispy.httpd.DispyHTTPServer(cluster)
    jobs=[]
    code_lists=os.listdir("/srv/work/D_stock/ticks")
    for x in code_lists:
        job=cluster.submit(x)
        job.id=int(x)
        logger.info(str(x))
        jobs.append(job)

    for job in jobs:
        host,n=job()
        logger.info('%s executed job %s at %s with stock-code %s'%(host,job.id,job.start_time,n))
    cluster.wait()
    http_server.shutdown()
    cluster.print_status()
