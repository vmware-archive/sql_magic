import argparse
import sys

import pandas.io.sql as psql

from IPython.core.display import display_javascript

from .exceptions import EmptyResult

try:
    from traitlets import TraitError
except ImportError:
    from IPython.utils.traitlets import TraitError

class Connection(object):

    def __init__(self, available_connection_types, no_return_result_exceptions):
        self.available_connection_types = available_connection_types
        self.no_return_result_exceptions = no_return_result_exceptions

    def is_an_available_connection(self, connection):
        return isinstance(connection, tuple(self.available_connection_types))

    # def is_a_sql_db_connection(self, connection):
    #     # must follow Python Database API Specification v2.0
    #     return isinstance(connection, tuple(self.available_connections))

    def is_a_spark_connection(self, connection):
        if 'pyspark' not in sys.modules:  # pyspark isn't installed
            return False
        return type(connection).__module__.startswith('pyspark')

    def _psql_read_sql_to_df(self, conn_object):
        def read_sql(sql_code):
            try:
                return psql.read_sql(sql_code, conn_object)
            except(tuple(self.no_return_result_exceptions)):
                import warnings
                return EmptyResult()
        return read_sql

    def _spark_call(self, conn_object):
        return lambda sql_code: conn_object.sql(sql_code).toPandas()

    def read_connection(self, conn_object):
        if self.is_a_spark_connection(conn_object):
            caller = self._spark_call(conn_object)
        else:
            caller = self._psql_read_sql_to_df(conn_object)
        return caller

    def validate_conn_object(self, conn_name, shell):
        try:
            proposal_value = shell.user_global_ns[conn_name]
            self.is_an_available_connection(proposal_value)
        except:
            raise TraitError('Connection name "{}" not recognized'.format(conn_name))
        return conn_name

def add_syntax_coloring():
    js_sql_syntax = '''
    require(['notebook/js/codecell'], function(codecell) {
      // https://github.com/jupyter/notebook/issues/2453
      codecell.CodeCell.options_default.highlight_modes['magic_text/x-sql'] = {'reg':[/^%read_sql/, /^%%read_sql/]};
      Jupyter.notebook.events.one('kernel_ready.Kernel', function(){
          console.log('BBBBB');
          Jupyter.notebook.get_cells().map(function(cell){
              if (cell.cell_type == 'code'){ cell.auto_highlight(); } }) ;
      });
    });
    '''
    display_javascript(js_sql_syntax, raw=True)

def create_flag_parser():
    ap = argparse.ArgumentParser()
    ap.add_argument('-n', '--notify', help='Toggle option for notifying query result', action='store_true')
    ap.add_argument('-a', '--async', help='Run query in seperate thread. Please be cautious when assigning\
                                           result to a variable', action='store_true')
    ap.add_argument('-d', '--display', help='Toggle option for outputing query result', action='store_true')
    ap.add_argument('-c', '--connection', help='Specify connection object for this query (override default\
                                                connection object)', action='store', default=False)
    ap.add_argument('table_name', nargs='?')
    return ap

def parse_read_sql_args(line_string):
    ap = create_flag_parser()
    opts = ap.parse_args(line_string.split())
    return {'table_name': opts.table_name, 'display': opts.display, 'notify': opts.notify,
            'async': opts.async, 'force_caller': opts.connection}


