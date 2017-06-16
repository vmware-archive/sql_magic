import ntpath
import sys
import time

import pandas as pd
import pytest

import sql_magic
from sql_magic.exceptions import EmptyResult

from IPython import get_ipython
from sqlalchemy import create_engine
from sqlite3 import dbapi2 as sqlite
from utils import create_spark_conn

ip = get_ipython()
ip.register_magics(sql_magic.SQL)
sql_magic.load_ipython_extension(ip)

# TODO: NAME to use, db.conn.__name__


connections = []

sqlite_conn = create_engine('sqlite+pysqlite:///test.db', module=sqlite)
conn_name = 'sqlite_conn'
ip.user_global_ns[conn_name] = sqlite_conn
connections.append(conn_name)

try:
    conn_name = 'spark'
    ip.user_global_ns[conn_name] = create_spark_conn()
    connections.append(conn_name)
except:
    sys.stderr.write('Spark not properly configured')

def test_spark_configured():
    assert 'spark' in connections

@pytest.fixture(scope="module",params=connections)
def conn(request):
    conn_name = request.param
    ip.run_line_magic('config', "SQL.conn_name = '{conn_name}'".format(conn_name=conn_name))
    assert ip.run_line_magic('config', 'SQL.conn_name') == conn_name
    return conn_name
    #TODO teardown if spark

def test_query_1(conn):
    ip.run_cell_magic('read_sql', 'df', 'SELECT 1')
    df = ip.user_global_ns['df']
    assert df.iloc[0, 0] == 1
    df = None

def test_line_magic_query_1(conn):
    df = ip.run_line_magic('read_sql', 'SELECT 123')
    assert df.iloc[0, 0] == 123
    df = None

def test_python_variable(conn):
    val = 'this is a python variable'
    ip.user_global_ns['val'] = val
    ip.run_cell_magic('read_sql', 'df', "SELECT '{val}'")
    df = ip.user_global_ns['df']
    assert df.iloc[0, 0] == val

def test_query_1_async(conn):
    ip.run_cell_magic('read_sql', 'df -a', 'SELECT "async_query"')
    df = ip.user_global_ns['df']
    assert isinstance(df, str) and (df == 'QUERY RUNNING')
    time.sleep(0.1)  # need to wait for query to finish
    df = ip.user_global_ns['df']
    assert df.iloc[0, 0] == 'async_query'

def test_query_1_notify(conn):
    ip.run_cell_magic('read_sql', 'df -n', 'SELECT 1')
    df = ip.user_global_ns['df']
    assert df.iloc[0, 0] == 1

def test_no_result(conn):
    ip.run_cell_magic('read_sql', '_df', 'DROP TABLE IF EXISTS test;')
    ip.run_cell_magic('read_sql', '_df', 'CREATE TABLE test AS SELECT 1')
    ip.run_cell_magic('read_sql', '_df', 'DROP TABLE IF EXISTS test;')
    _df = ip.user_global_ns['_df']
    # pyspark
    assert isinstance(_df, EmptyResult)

# def test_invalud_conn_object(sqlite_conn):
#     with pytest.raises(message="Expecting ZeroDivisionError"):
#         ip.run_line_magic('config', "SQL.conn_name = 'invalid_conn'")

# def test_commented_query(sqlite_conn):
#     sql_statement = '''
#     /* DROP TABLE IF EXISTS TEST; */
#     /* CREATE TABLE TEST AS SELECT 1; */
#     /* WITH test AS (SELECT 1) */
#     /* SELECT 2 */
#     '''
#     assert 1 == 2

def test_second_conn_object(conn):
    conn2 = create_engine('sqlite+pysqlite:///test2.db', module=sqlite)
    ip.user_global_ns['conn2'] = conn2
    ip.run_cell_magic('read_sql', 'df -c conn2', 'PRAGMA database_list;')
    df = ip.user_global_ns['df']
    assert ntpath.basename(df.file.iloc[0]) == 'test2.db'
    assert ip.run_line_magic('config', "SQL.conn_name") == conn  # check original connection

def test_query_with(conn):
    ip.run_cell_magic('read_sql', 'df', 'WITH test AS (SELECT 1) SELECT 2')
    df = ip.user_global_ns['df']
    assert df.iloc[0, 0] == 2

def test_multiple_sql_statements_var(conn):
    sql_statement = '''
    DROP TABLE IF EXISTS TEST;
    SELECT 1;
    SELECT 2;
'''
    ip.run_cell_magic('read_sql', 'df3', sql_statement)
    df3 = ip.user_global_ns['df3']
    assert df3.iloc[0, 0] == 2

def test_multiple_sql_statements_no_result(conn):
    ip.run_cell_magic('read_sql', '', 'DROP TABLE IF EXISTS test;')
    ip.run_cell_magic('read_sql', '', 'CREATE TABLE test AS SELECT 2;')
    ip.run_cell_magic('read_sql', '', 'SELECT * FROM test')
    ip.run_cell_magic('read_sql', '_df', 'DROP TABLE IF EXISTS test;')
    _df = ip.user_global_ns['_df']
    assert isinstance(_df, EmptyResult)
    # assert df2.iloc[0, 0] == 2
