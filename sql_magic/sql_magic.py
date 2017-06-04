import argparse
import sys
import time
import threading

import pandas.io.sql as psql
import sqlparse

from IPython.core.display import display_javascript
from IPython.core.magic import Magics, magics_class, cell_magic

from notify import Notify

try:
    from traitlets.config.configurable import Configurable
    from traitlets import observe, validate, Bool, Unicode, TraitError
except ImportError:
    from IPython.config.configurable import Configurable
    from IPython.utils.traitlets import observe, validate, Bool, Unicode, TraitError

AVAILABLE_CONNECTIONS = []
DEFAULT_OUTPUT_RESULT = True
DEFAULT_NOTIFY_RESULT = True

no_return_result_exceptions = []  # catch exception if user used read_sql where query returns no result

try:
    import pyspark
    AVAILABLE_CONNECTIONS.append(pyspark.sql.context.SQLContext)
    AVAILABLE_CONNECTIONS.append(pyspark.sql.context.HiveContext)
    AVAILABLE_CONNECTIONS.append(pyspark.sql.session.SparkSession)  # import last;  will fail for older spark versions
except:
    pass
try:
    import psycopg2
    AVAILABLE_CONNECTIONS.append(psycopg2.extensions.connection)
    no_return_result_exceptions.append(TypeError)
except:
    pass
try:
    import sqlite3
    AVAILABLE_CONNECTIONS.append(sqlite3.Connection)
except:
    pass
try:
    import sqlalchemy
    AVAILABLE_CONNECTIONS.append(sqlalchemy.engine.base.Engine)
    no_return_result_exceptions.append(sqlalchemy.exc.ResourceClosedError)
except:
    pass


def is_an_available_connection(connection):
    return isinstance(connection, tuple(AVAILABLE_CONNECTIONS))


def is_a_spark_connection(connection):
    if 'pyspark' not in sys.modules:  # pyspark isn't installed
        return False
    return type(connection).__module__.startswith('pyspark')


def is_a_sql_db_connection(connection):
    # must follow Python Database API Specification v2.0
    return isinstance(connection, tuple(AVAILABLE_CONNECTIONS))

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

@magics_class
class SQLConn(Magics, Configurable):

    # traits, configurable using %config
    conn_object_name = Unicode("", help="Object name for accessing computing resource environment").tag(config=True)
    output_result = Bool(DEFAULT_OUTPUT_RESULT, help="Output query result to stdout").tag(config=True)
    notify_result = Bool(DEFAULT_NOTIFY_RESULT, help="Notify query result to stdout").tag(config=True)

    def __init__(self, shell):
        # access shell environment
        self.shell = shell
        self.jupyter_namespace = shell.all_ns_refs[0]

        Configurable.__init__(self, config=shell.config)
        Magics.__init__(self, shell=shell)

        self.notify_obj = Notify(shell)

        # Add to the list of module configurable via %config
        self.shell.configurables.append(self)

    def _psql_read_sql_to_df(self, sql_code):
        try:
            return psql.read_sql(sql_code, self.conn_object)
        except(tuple(no_return_result_exceptions)):
            import warnings
            # warnings.warn('hello')
            return EmptyResult()
            # raise NoReturnValueResult("Query doesn't return a result; please use %%exec_sql")

    def _psql_exec_sql(self, sql_code):
        return psql.execute(sql_code, self.conn_object)

    def _spark_call(self, sql_code):
        return self.conn_object.sql(sql_code).toPandas()

    @validate('output_result')
    def _validate_output_result(self, proposal):
        try:
            return bool(proposal['value'])
        except:
            raise TraitError('output_result: "{}" is not accepted. Value must be boolean'.format(proposal['value']))

    @validate('conn_object_name')
    def _validate_conn_object_name(self, proposal):
        jupyter_namespace = self.shell.all_ns_refs[0]
        if proposal['value'] not in jupyter_namespace.keys():
            raise TraitError('Connection name "{}" not recognized'.format(proposal['value']))
        proposal_value = self.jupyter_namespace[proposal['value']]

        if not is_an_available_connection(proposal_value):
            raise TraitError('Connection name "{}" not recognized'.format(proposal['value']))
        return proposal['value']

    @observe('conn_object_name')
    def config_connection(self, change):
        new_conn_object_name = change['new']
        conn_object = self.jupyter_namespace[new_conn_object_name]
        self.conn_object = conn_object
        if is_a_spark_connection(conn_object):
            caller = self._spark_call
        else:
            caller = self._psql_read_sql_to_df

        self.caller = caller

    def _create_flag_parser(self, line_string):
        ap = argparse.ArgumentParser()
        ap.add_argument('-n', '--notify', help='Toggle option for notifying query result', action='store_true')
        ap.add_argument('-a', '--async', help='Run query in seperate thread. Please be cautious when assigning\
                                               result to a variable', action='store_true')
        return ap

    def _parse_read_sql_args(self, line_string):
        ap = self._create_flag_parser(line_string)
        ap.add_argument('-d', '--display', help='Toggle option for outputing query result', action='store_true')
        ap.add_argument('table_name', nargs='?')
        opts = ap.parse_args(line_string.split())
        return {'table_name': opts.table_name, 'display': opts.display, 'notify': opts.notify, 'async': opts.async}

    def _parse_exec_sql_args(self, line_string):
        ap = self._create_flag_parser(line_string)
        opts = ap.parse_args(line_string.split())
        return {'notify': opts.notify, 'async': opts.async}

    def _time_query(self, caller, sql):
        # time results and output
        pretty_start_time = time.strftime('%I:%M:%S %p %Z')
        # self.shell.displayhook(HTML('<p style="color:gray">Query started at {}</p>'.format(pretty_start_time)))
        print('Query started at {}'.format(pretty_start_time))
        start_time = time.time()
        result = caller(sql)
        end_time = time.time()
        del_time = (end_time-start_time)/60.
        # self.shell.displayhook(HTML('<p style="color:gray">Query executed in {:2.2f} m</p>'.format(del_time)))
        print('Query executed in {:2.2f} m'.format(del_time))
        return result, del_time

    def _read_sql_engine(self, sql, table_name, show_output, notify_result):
        self.shell.all_ns_refs[0][table_name] = 'QUERY RUNNING'
        try:
            result, del_time = self._time_query(self.caller, sql)
        except Exception as e:  # pandas' read_sql/sqlalchemy complains if no result
            print(str(e))
            no_result_error = (str(e) == 'This result object does not return rows. It has been closed automatically.')
            if not no_result_error:
                raise Exception(e)
        if table_name:
            # add to iPython namespace
            #TODO: self.shell.user_ns.update({result_var: result})
            self.shell.all_ns_refs[0][table_name] = result
        query_has_result = not isinstance(result, EmptyResult)
        if show_output and query_has_result:
            self.shell.displayhook(result)
        if notify_result:
            self.notify_obj.notify_complete(del_time, table_name, result.shape)

    def _exec_sql_engine(self, sql, notify_result):
        result, del_time = self._time_query(self._psql_exec_sql, sql)

        if notify_result:
            self.notify_obj.notify_complete(del_time, 'Execute SQL', None)

    @cell_magic
    # def _parse_and_run_sql(self, line, cell):
    def read_sql(self, line, cell):
        user_args = self._parse_read_sql_args(line)
        table_name, toggle_display, toggle_notify, async = [user_args[k] for k in ['table_name', 'display',
                                                                                   'notify', 'async']]
        sql = cell.format(**self.jupyter_namespace)
        show_output = self.output_result ^ toggle_display
        notify_result = self.notify_result ^ toggle_notify
        statements = [s for s in sqlparse.split(sql) if s]
        if async:
            if len(statements) > 1:
                raise AsyncError('Only one SQL statement allowed in async queries')
            else:
                async_show_output = False ^ toggle_display  # default to False
                t = threading.Thread(target=self._read_sql_engine, args=[sql, table_name, async_show_output,
                                                                         notify_result])
                t.start()
        else:
            for i, s in enumerate(statements, start=1):
                self._read_sql_engine(s, table_name, show_output, notify_result)

class NoReturnValueResult(Exception):
    pass

class AsyncError(Exception):
    pass

class EmptyResult(object):
    shape = None

    def __str__(self):
        return ''

def load_ipython_extension(ip):
    """Load the extension in IPython."""
    # add syntax coloring
    ip.register_magics(SQLConn)

def unload_ipython_extension(ip):
    # fix how it loads multiple times
    if 'SQLConn' in ip.magics_manager.registry:
        del ip.magics_manager.registry['SQLConn']
    # del ip.magics_manager.magics['cell']['read_sql']
    if 'SQLConn' in ip.config:
        del ip.config['SQLConn']