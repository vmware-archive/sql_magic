import ntpath
import time

import pytest
import pandas as pd
import sql_magic

from IPython import get_ipython
from sqlalchemy import create_engine
from sqlite3 import dbapi2 as sqlite

ip = get_ipython()
ip.register_magics(sql_magic.SQLConn)
sql_magic.load_ipython_extension(ip)

# TODO: Can we test these using multiple parameters? I.E, SparkSQL


@pytest.fixture
def sqlite_conn():
    conn = create_engine('sqlite+pysqlite:///test.db', module=sqlite)
    ip.all_ns_refs[0]['conn'] = conn

def test_query_1(sqlite_conn):
    ip.run_line_magic('config', "SQLConn.conn_object_name = 'conn'")
    ip.run_cell_magic('read_sql', 'df', 'SELECT 1')
    df = ip.all_ns_refs[0]['df']
    assert df.iloc[0,0] == 1

def test_query_1_async(sqlite_conn):
    ip.run_line_magic('config', "SQLConn.conn_object_name = 'conn'")
    ip.run_cell_magic('read_sql', 'df -a', 'SELECT "async_query"')
    df = ip.all_ns_refs[0]['df']
    query_still_running = isinstance(df, str) and (df == 'QUERY RUNNING')
    assert query_still_running
    time.sleep(0.1)  # need to wait for query to finish
    df = ip.all_ns_refs[0]['df']
    assert df.iloc[0, 0] == 'async_query'

def test_query_1_notify(sqlite_conn):
    ip.run_line_magic('config', "SQLConn.conn_object_name = 'conn'")
    ip.run_cell_magic('read_sql', 'df -n', 'SELECT 1')
    df = ip.all_ns_refs[0]['df']
    assert df.iloc[0, 0] == 1

def test_second_conn_object(sqlite_conn):
    # test original
    ip.run_line_magic('config', "SQLConn.conn_object_name = 'conn'")
    ip.run_cell_magic('read_sql', 'df', 'PRAGMA database_list;')
    df = ip.all_ns_refs[0]['df']
    assert ntpath.basename(df.file.iloc[0]) == 'test.db'

    # test new connection
    conn2 = create_engine('sqlite+pysqlite:///test2.db', module=sqlite)
    ip.all_ns_refs[0]['conn2'] = conn2
    # ip.run_line_magic('config', "SQLConn.conn_object_name = 'conn2'")
    ip.run_cell_magic('read_sql', 'df -c conn2', 'PRAGMA database_list;')
    df = ip.all_ns_refs[0]['df']
    assert ntpath.basename(df.file.iloc[0]) == 'test2.db'

    # make sure with no argument stays connected to original database
    ip.run_cell_magic('read_sql', 'df', 'PRAGMA database_list;')
    df = ip.all_ns_refs[0]['df']
    connected_to_orig_db = ntpath.basename(df.file.iloc[0]) == 'test.db'
    assert connected_to_orig_db


def test_no_result(sqlite_conn):
    ip.run_line_magic('config', "SQLConn.conn_object_name = 'conn'")
    ip.run_cell_magic('read_sql', '_df', 'DROP TABLE IF EXISTS test;')
    _df = ip.all_ns_refs[0]['_df']
    assert isinstance(_df, sql_magic.EmptyResult)

def test_query_with(sqlite_conn):
    ip.run_line_magic('config', "SQLConn.conn_object_name = 'conn'")
    ip.run_cell_magic('read_sql', 'df', 'WITH test AS (SELECT 1) SELECT 2')
    df = ip.all_ns_refs[0]['df']
    assert df.iloc[0, 0] == 2

def test_multiple_sql_statements_var(sqlite_conn):
    sql_statement = '''
    DROP TABLE IF EXISTS TEST;
    SELECT 1;
    SELECT 2;
    '''
    ip.run_cell_magic('read_sql', 'df3', sql_statement)
    df3 = ip.all_ns_refs[0]['df3']
    assert df3.iloc[0, 0] == 2

def test_multiple_sql_statements_no_result(sqlite_conn):
    ip.run_cell_magic('read_sql', '', 'DROP TABLE IF EXISTS test;')
    ip.run_cell_magic('read_sql', '', 'CREATE TABLE test AS SELECT 2;')
    ip.run_cell_magic('read_sql', '', 'SELECT * FROM test')
    ip.run_cell_magic('read_sql', '_df', 'DROP TABLE IF EXISTS test;')
    _df = ip.all_ns_refs[0]['_df']
    assert isinstance(_df, sql_magic.EmptyResult)
    # assert df2.iloc[0, 0] == 2

def test_async_multiple_queries(sqlite_conn):
    with pytest.raises(sql_magic.AsyncError):
        sql_statement = '''
        DROP TABLE IF EXISTS TEST;
        SELECT 1;
        SELECT 2;
        '''
        ip.run_cell_magic('read_sql', '-a', sql_statement)

