
from __future__ import absolute_import

import datetime
import logging
import multiprocessing
import time
import influxdb
import pandas as pd
import requests

LOG = logging.getLogger(__name__)

"""
Using multiprocessing to retrieve data from influxdb.
Each manager manages its own workers (instance of Process)
"""

DEV_002_DB_HOST = {
    'host': 'example.com',
    'port': 8086,
    'username': 'root',
    'password': 'root'
}

class SimpleMgr(object):
    """retrieve the data and do analysis for a string"""

    def __init__(self, db_host_name, database, rp, string_count, string_len, window):
        self._db_host = DEV_002_DB_HOST.copy()
        self._db_host['host'] = db_host_name
        self._database = database
        self._rp = rp
        self._string_count = string_count
        self._string_len = string_len
        self._window = window
        self._begin = pd.Timestamp(datetime.datetime.utcfromtimestamp(self._window['start_ms'] / 1000))
        self._end = pd.Timestamp(datetime.datetime.utcfromtimestamp(self._window['end_ms'] / 1000))
        self._qDays = 2
        self._resSecs = 5

        self._stringplot_worker = SimpleWorker(database, self._db_host)
        self._battplot_worker_list = {}

        for i in range(self._string_len):
            battplot_worker = SimpleWorker(database, self._db_host)
            self._battplot_worker_list[str(i)] = battplot_worker

    def start_analyze(self):
        da_res = {
            'voltage_spread': {},
            'efficiency': {}
        }
        # spawn all the processes
        self.start_processing()

        for i in range(self._string_count):
            # send commands of current string to all worker processes
            sid = i + 1
            stringplot_queries = self.generate_queries('stringplot', "id='{}'".format(sid))
            self._stringplot_worker.parent_conn.send(stringplot_queries)

            bid_offset = self._string_len * (sid - 1)
            for b_index in range(self._string_len):
                bid = bid_offset + b_index
                battplot_queries = self.generate_queries('battplot', "bay='{}'".format(bid))
                self._battplot_worker_list[str(b_index)].parent_conn.send(battplot_queries)
            
            da_single_result = self.analyze_single_string(sid)
            if da_single_result is not None:
                # update the ayalysis result of current string into the da_res
                for measurement in da_res:
                    da_res[measurement][str(sid)] = da_single_result[measurement]
        
        # stop all processes after it's done
        self._stringplot_worker.parent_conn.send('STOP')
        self._stringplot_worker.join()

        for worker in self._battplot_worker_list.values():
            worker.parent_conn.send('STOP')
            worker.join()
        
        return da_res

    def analyze_single_string(self, sid):
        stringplot_result = self._stringplot_worker.wait_result()

        batt_data = {}
        for bid, battplot_worker in self._battplot_worker_list.iteritems():
            battplot_result = battplot_worker.wait_result()
            batt_data[bid] = battplot_result
        
        return stringplot_result, batt_data
    
    def generate_queries(self, measurement, where_cond):
        """generate query strings for a given measurement"""
        queries = []
        measurement = self._rp + '.' + measurement
        begin = self._begin
        endQ = self._end

        while endQ > begin:
            beginQ = endQ - pd.Timedelta(str(self._qDays)+'d')
            if beginQ < begin:
                beginQ = begin

            qStr = "SELECT * FROM " + measurement + " WHERE " + where_cond + " AND "

            if self._end.tz:
                qStr += ("time >= '"+beginQ.isoformat()[:-6]+"Z' AND time < '"+endQ.isoformat()[:-6]+"Z'")
            else:
                qStr += ("time >= '"+beginQ.isoformat()+"Z' AND time < '"+endQ.isoformat()+"Z'")
            
            queries.append(qStr)
            endQ = beginQ

        return queries

    def start_processing(self):
        self._stringplot_worker.start_processing()

        for worker in self._battplot_worker_list.values():
            worker.start_processing()


class SimpleWorker(multiprocessing.Process):
    """docstring for InfluxWorker"""

    def __init__(self, database, db_host=DEV_002_DB_HOST):
        """
        Args:
            queries: a list of query expressions
        """
        assert 'host' in db_host
        assert 'port' in db_host
        assert 'username' in db_host
        assert 'password' in db_host

        multiprocessing.Process.__init__(self)
        self._db_host = db_host
        self._database = database
        # self._queries = queries

        self._client = influxdb.DataFrameClient(host=self._db_host['host'],
                                                port=self._db_host['port'],
                                                database=database,
                                                timeout=90,
                                                username=self._db_host['username'],
                                                password=self._db_host['password'])
        # self._queue = multiprocessing.Queue()
        self.parent_conn, self._child_conn = multiprocessing.Pipe()

    def run(self):
        # LOG.debug("worker run")
        print multiprocessing.current_process().name
        self.setup()
        begin_time = time.time()

        while True:
            df = pd.DataFrame()
            queries = self._child_conn.recv()
            print 'child recieved: ' + str(queries)

            if queries == 'STOP':
                # recieve a STOP command, it's time to quick
                break

            for qStr in queries:
                # print qStr
                try:
                    print str(self.pid) + ' is child process alive: ' + str(self.is_alive())
                    dicTemp = self._client.query(qStr, chunked = True)
                    if dicTemp: #Check that there is a response
                        if len(dicTemp)>1: #if response is groupby type, place all columns in a single dataframe
                            print 'Error: why is the result grouped by'
                        else:
                            df=df.append(dicTemp.values()[0])
                except (AttributeError, requests.exceptions.ConnectionError) as e:
                    print e

            df.sort_index(inplace=True)
            self._child_conn.send(df)
            del df

        self._child_conn.close()

    def start_processing(self):
        self.start()

    def wait_result(self):
        """This method can only be called by a parent process to recieve data"""
        print multiprocessing.current_process().name
        while True:
            if self.parent_conn.poll(1):
                self._result = self.parent_conn.recv()
                # self._result.sort_index(inplace=True)
                return self._result
            else:
                time.sleep(1)

    def setup(self):
        pass

    def get_queries(self):
        return self._queries
