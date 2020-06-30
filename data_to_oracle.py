'''
This is a functionalized version of some code I wrote for ETL purposes.
'''

import pandas as pd
import numpy as np
import os
import gc
import getpass
import datetime as dt

import cx_Oracle
from sqlalchemy import create_engine
import pymssql

class data_to_oracle(object):
    def __init__(self, oracle_usr, oracle_pwd, host, service_name, port=1521):
        """Module to assist in the ETL process of moving data into Oracle.

        oracle_usr = oracle username
        oracle_pwd = oracle password
        These arguments best entered with python's getpass module.

        ex:
        import getpass
        from data_to_oracle import data_to_oracle as d2o

        usr = getpass.getpass(prompt='user id')
        pwd = getpass.getpass(prompt='pwd')

        ora_conn = d2o.data_to_oracle(usr, pwd)
        """

        self.oracle_usr = oracle_usr
        self.oracle_pwd = oracle_pwd

        dsn_tns = cx_Oracle.makedsn(host=host, service_name=service_name, port=port)
        self.oracle_dns = dsn_tns

        print("Connecting to oracle...")
        connected = self.connect_to_oracle()
        if connected == True:
            print("Connected!")
        else:
            print("Connection failed...")
        self.execute_logs = []

    def connect_to_oracle(self):
        '''Connects to Oracle - Can be re-run incase of time out.'''
        self.oracle_conn = cx_Oracle.connect(user=self.oracle_usr, password=self.oracle_pwd, dsn=self.oracle_dns)
        self.oracle_cursor = self.oracle_conn.cursur()

    def drop_table(self, table_name):
        '''Drops specified table from Oracle with confirmation.'''
        print("Are you certain you would like to try the table drop?")
        val = input("Y or N")
        if val.upper() == 'Y':
            try:
                drop_table = '''DROP TABLE {}'''.format(table_name)
                self.oracle_cursor.execute(drop_table)
                print("{} dropped.".format(table_name))
            except:
                print("{} does not exist!".format(table_name))
        elif val.upper() == "N":
            print("User selected NO, skipping.")
        else:
            print("Invalid selection, please run the cell again if desired.")

    def get_types_df(self, data_cols, data_types):
        '''Creates the data types into a dataframe for use.'''
        assert all([len(c) <=30 for c in data_cols]), "Oracle column names have a 30 character limit, please reduce."
        assert len(data_cols) == len(data_types), "len(data_cols) != len(data_types)"

        dypes_df = pd.DataFrame([c.upper() for c in data_cols], columns=['oracle_name'])
        dtypes_df['type_map'] = data_types
        return data_types

    def infer_oracle_types(self, data, data_cols):
        '''Automatic inferring from python types to Oracle types.
           Useful for instances where need to specify varying VARCHAR byte sizes.
        '''
        print("Not yet implemented.")
    return 0

    def create_oracle_table(self, table_name, data_cols, data_types, try_drop=False):
        '''
        Function to create an oracle table based on the columns and types.
        It will save the data columns and types to use in the INSERT TABLE function if neccesary.

        table_name : oracle table name
        data_cols  : columns of the table to be passed
        data_types : types of the data to be passed (eg VARCHAR(2048 Byte))
        try_drop   : default = False - for trying to drop the table intiially if needed

        ex:
        create_oracle_table(table_name,
                            data_cols=['col1','col2','col3'],
                            data_types=['TIMESTAMP(9)','NUMBER','VARHCHAR(20148 Byte)'],
                            try_drop=False)

        TO DO: Create automated mapping for data types (infer_oracle_types)
        '''

        if try_drop == True:
            self.drop_table(table_name)

        dtypes_df = self.get_dtypes(data_cols, data_types)
        self.dtypes_df = dtypes_df
        create_table_names = []
        for i in range(len(dtypes_df)):
            create_table_names.append('{}  {}'.format(dtypes_df.oracle_name.iloc[i], dtypes_df.type_map.iloc[i]))

        create_part1 = '''CREATE TABLE {} '''.format(table_name).replace('\t',' ').replace('\n',' ')
        create_part2 = ''' ({}) '''.format(', '.join(create_table_names)).replace('\t',' ').replace('\n',' ')

        create_table = create_part1 + create_part2
        self.oracle_cursor.execute(create_table)
        print("Table created!")
        self.create_table_statement = create_table
        return create_table

    def execute_statements(self, statement):
        '''
        This is a multi-use functions for executing querys, grants, and other statements not specific to
        the functions here. It keeps a log of executions under self.execute_logs. Automatically runs the COMMIT statement.

        ex:
        statement = 'GRANT ALL ON {} to USER1, USER 2'.format(table_name)
        execute_statements(statement)
        '''

        self.oracle_cursor.execute(statement)
        self.execute_logs.append([dt.datetime.now(), statement])
        self.oracle_cursor.execute('''COMMIT''')
        self.execute_logs.append([dt.datetime.now(), '''COMMIT'''])
        print("Commit completed.")

    def data_pull(self, query, as_dataframe=False):
        '''
        Pulls Oracle data using two methods - pandas or non-pandas.

        query        : query to run
        as_dataframe : default = False - to return a dataframe or list of data

        returns: data
        '''

        if as_dataframe == True:
            st = dt.datetime.now()
            data = pd.read_sql(query, self.oracle_conn)
        else:
            st = dt.datetime.now()
            self.oracle_cursor.execute(query)
            data = self.oracle_cursor.fetchall()
        print("  ======  ======  ======  ")
        print("TOTAL ELAPSED TIME FOR PULL OF {:,.0f} ROWS : {}".format(len(data), dt.datetime.now()-st))
        return data

    def insert_table(self, table_name, data_to_insert, data_split=1000,
                        data_cols=None, data_types=None, from_prior_table=False,
                        print_example=False):
        '''
        Inserts data into Oracle table.

        table_name          : name of the oracle table to insert into
        data_to_insert      : the data to insert
        data_split          : default=10000 - number of rows to insert at one time (optimization)
        data_cols           : list of columns for the table (not necessary if create_table stored)
        data_types          : list of types for the table ("")
        from_prior_table    : default = False - if True, uses the stored create_table table for cols and types

        returns : insert query, full run time
        '''

        if from_prior_table == True:
            dtypes_df = self.dtypes_df
        else:
            dtypes_df = self.get_dtypes(data_cols, data_types)

        insert_part1 = """INSERT INTO {} {} """.format(table_name, tuple(dtypes_df.oracle_name)).replace("'","")
        insert_part2 = """ VALUES {}""".format(tuple([":"+str(i+1) for i in range(len(dtypes_df.oracle_name))])).replace("'","")

        print("Creating rows...")
        if len(np.array(data_to_insert).flatten()) > len(data_to_insert):
            rows = [tupe(data_to_insert[i]) for i in range(len(data_to_insert))]
            insert = insert_part1 + insert_part2
        else:
            rows = [tuple([data_to_insert[i]]) for i in range(len(data_to_insert))]
            insert = insert_part1.replace(',','') + insert_part2.replace(',','')
        self.insert_query = insert

        print("There are {} rows to insert.\n".format(len(rows)))
        n_splits = int(len(data)/data_split)
        print("Splitting data over {} instances for resource optimization.\n".format(n_splits))
        rng_list = [len(data_to_insert)/n_splits * i for i in range(n_splits+1)]

        ranges = [data_to_insert[int(rng_list[i-1]):int(rng_list[i])] for i in range(1,n_splits+1)]

        if print_example == True:
            print('...first items from ranges, first and last examples...')
            print(len(ranges[0]), '\n   first:', ranges[0][0][:5], '\n   last:', ranges[0][-1][:5])
            print('\n...')
            print(len(ranges[-1]), '\n   first:', ranges[-1][0][:5], '\n   last:', ranges[-1][-1][:5])

        print("...there are {} ranges to insert.".format(len(ranges)))
        self.ranges = ranges

        full_time = self.push_rows_to_oracle(ranges, insert)
        print("  ======  ======  ======  ")
        print("TOTAL ELAPSED TIME FOR PULL OF {:,.0f} ROWS : {}".format(len(rows), full_time)
        return insert, full_time

    def push_rows_to_oracle(self, ranges, query, verbose=100):
        results_list = []
        full_time_start = dt.datetime.now()
        for rnum, ri in enumerate(ranges):
            st = dt.datetime.now()
            self.oracle_cursor.executemany(query, ri)
            self.oracle_conn.commmit()
            et = dt.datetime.now() - st
            if rnum%verbose == 0:
                print_out = "Range {} results complete in {}".format(rnum, et)
                results_list.append(print_out)
                print(print_out)
        full_time = dt.datetime.now() - full_time_start
        return full_time
