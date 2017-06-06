import argparse
import sys

from IPython.core.display import display_javascript


class ConnValidation(object):

    def __init__(self, available_connections):
        self.available_connections = available_connections

    def is_an_available_connection(self, connection):
        return isinstance(connection, tuple(self.available_connections))

    # def is_a_sql_db_connection(self, connection):
    #     # must follow Python Database API Specification v2.0
    #     return isinstance(connection, tuple(self.available_connections))

    def is_a_spark_connection(self, connection):
        if 'pyspark' not in sys.modules:  # pyspark isn't installed
            return False
        return type(connection).__module__.startswith('pyspark')

def add_syntax_coloring():
    js_sql_syntax = '''
    require(['notebook/js/codecell'], function(codecell) {
      // https://github.com/jupyter/notebook/issues/2453
      codecell.CodeCell.options_default.highlight_modes['magic_text/x-sql'] = {'reg':[/^%%read_sql/]};
      console.log('AAAAA');
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
