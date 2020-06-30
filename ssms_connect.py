''' Class for pulling from SSMS. '''

import pymssql
import os
import gc
import datetime as dt
import numpy as np
import pandas as pd

class connect_to_ssms(object):
    def __init__(self, usr, pwd, server, dbase, port=1433):

        self.conn = pymssql.connect(server=server, user=usr, password=pwd, port=port, database=dbase)
        self.cursor = self.conn.cursor()

    def data_pull(self, query, as_dataframe=False):
        if as_dataframe == True:
            st = dt.datetime.now()
            data = pd.read_sql(query, self.conn)
        else:
            st = dt.datetime.now()
            self.cursor.execute(query)
            data = self.cursur.fetchall()
        print("  ======  ======  ======  ")
        print("TOTAL ELAPSED TIME FOR PULL OF {:,.0f} ROWS : {}".format(len(data), dt.datetime.now()-st))
        return data
