#import ntpath
#import sys
#import time
#
#import pytest
#
#import sys
#sys.path.append('..')
#import sql_magic
#from sql_magic.exceptions import EmptyResult
#
#from IPython import get_ipython
#from sqlalchemy import create_engine
#from sqlite3 import dbapi2 as sqlite
#from utils import create_spark_conn
#
#ip = get_ipython()
#ip.register_magics(sql_magic.SQL)
#sql_magic.load_ipython_extension(ip)
#
#conn_name = 'spark'
#ip.user_global_ns[conn_name] = create_spark_conn()
#ip.run_line_magic('config', "SQL.conn_name = '{conn_name}'".format(conn_name=conn_name))
#ip.run_cell_magic('read_sql', '', 'CREATE TABLE test AS SELECT 2;')
