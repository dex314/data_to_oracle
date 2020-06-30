'''Class to connect to redshift.'''

import sqlalchemy
from sqlalchemy import create_engine
import os
import gc
import datetime as dt
import numpy as np
import pandas as pd

class connect_to_redshift(object):
    def __init__(self, usr, pwd, server, dbase, port=5439):

        conn_str = '''redshift+psycopg2://{}:{}@{}:{}/{}'''.format(user, pwd, server, port, dbase)
        self.engine = create_engine(conn_str)
        self.conn = self.engine.connect()

    def data_pull(self, query, as_dataframe=False):
        if as_dataframe == True:
            st = dt.datetime.now()
            data = pd.read_sql(query, self.conn)
        else:
            st = dt.datetime.now()
            q = self.conn.execute(query)
            data = q.fetchall()
        print("  ======  ======  ======  ")
        print("TOTAL ELAPSED TIME FOR PULL OF {:,.0f} ROWS : {}".format(len(data), dt.datetime.now()-st))
        return data
