import pytest
import pandas
import sql_magic
from traitlets import TraitError

from IPython import get_ipython
from sqlalchemy import create_engine
from sqlite3 import dbapi2 as sqlite

ip = get_ipython()
ip.register_magics(sql_magic.SQLConn)
sql_magic.load_ipython_extension(ip)

@pytest.fixture
def sqlite_conn():
    return create_engine('sqlite+pysqlite:///test.db', module=sqlite)

def test_query_1(sqlite_conn):
    conn = sqlite_conn
    ip.all_ns_refs[0]['conn'] = conn
    ip.run_line_magic('config', "SQLConn.conn_object_name = 'conn'")
    ip.run_cell_magic('readsql', 'df', 'SELECT 1')
    df = ip.all_ns_refs[0]['df']
    assert df.iloc[0,0] == 1

def test_query_1_notify(sqlite_conn):
    conn = sqlite_conn
    ip.all_ns_refs[0]['conn'] = conn
    ip.run_line_magic('config', "SQLConn.conn_object_name = 'conn'")
    ip.run_cell_magic('readsql', 'df -n', 'SELECT 1')
    df = ip.all_ns_refs[0]['df']
    assert df.iloc[0, 0] == 1

def test_readsql_create_table_error():
    non_valid_conn = 'BAD_CONN'
    with pytest.raises(TraitError):
        conn = sqlite_conn
        ip.all_ns_refs[0]['conn'] = conn
        ip.run_line_magic('config', "SQLConn.conn_object_name = 'conn'")
        ip.run_cell_magic('readsql', '', 'CREATE TABLE test AS SELECT 1')
# def setup():
#     sqlmagic = SqlMagic(shell=ip)
#     ip.register_magics(sqlmagic)