#!/usr/bin/env python
# coding=utf-8
import dispy, random
import logging
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

    cluster=dispy.JobCluster(compute,nodes=[('172.16.0.133',51348),('172.16.0.134',51348),('172.16.0.136',51348),('172.16.0.135',51348)])
    jobs=[]
    for i in range(20):
        job=cluster.submit(random.randint(5,20))
        job.id=i
        logger.info(str(i))
        jobs.append(job)


    for job in jobs:
        host,n=job()#???
       # print type(job)
        logger.info('%s executed job %s at %s with %s'%(host,job.id,job.start_time,n))

    cluster.print_status()

