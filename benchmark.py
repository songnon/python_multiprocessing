#!/usr/bin/env python 

import pandas as pd
import influxdb
import time
import multiprocessing as mp

import influxdbda
import simple

INFLUXDB_HOST_NAME = 'example.com'
INFLUXDB_DB = 'ABC' # a database inside influxdb

def concurrent_class():
    startTS = time.time()

    window = {
        'start_ms': 1511395200000, # 2017-11-23
        'end_ms': 1511827200000 # 2017-11-28
        }
        
    sm = simple.SimpleMgr(INFLUXDB_HOST_NAME, INFLUXDB_DB, 'rp7d', 8, 4, window)
    print sm.start_analyze()

    time_consumed = time.time() - startTS
    print 'seconds elapsed: ' + str(time_consumed)
    return time_consumed


if __name__ == '__main__':
    concurrent_class()