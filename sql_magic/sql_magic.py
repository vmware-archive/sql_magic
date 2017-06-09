import threading

import sqlparse
from IPython.core.magic import Magics, magics_class, cell_magic, needs_local_scope

from . import engine
from . import utils

from .notify import Notify
from .exceptions import EmptyResult

try:
    from traitlets.config.configurable import Configurable
    from traitlets import observe, validate, Bool, Unicode, TraitError
except ImportError:
    from IPython.config.configurable import Configurable
    from IPython.utils.traitlets import observe, validate, Bool, Unicode, TraitError


available_connection_types = []
no_return_result_exceptions = []  # catch exception if user used read_sql where query returns no result
try:
    import pyspark
    available_connection_types.append(pyspark.sql.context.SQLContext)
    available_connection_types.append(pyspark.sql.context.HiveContext)
    available_connection_types.append(pyspark.sql.session.SparkSession)  # import last;  will fail for older spark versions
except:
    pass
try:
    import psycopg2
    available_connection_types.append(psycopg2.extensions.connection)
    no_return_result_exceptions.append(TypeError)
except:
    pass
try:
    import sqlite3
    available_connection_types.append(sqlite3.Connection)
except:
    pass
try:
    import sqlalchemy
    available_connection_types.append(sqlalchemy.engine.base.Engine)
    no_return_result_exceptions.append(sqlalchemy.exc.ResourceClosedError)
except:
    pass


conn_val = utils.ConnValidation(available_connection_types, no_return_result_exceptions)
DEFAULT_OUTPUT_RESULT = True
DEFAULT_NOTIFY_RESULT = True
@magics_class
class SQLConn(Magics, Configurable):

    # traits, configurable using %config
    conn_object_name = Unicode("", help="Object name for accessing computing resource environment").tag(config=True)
    output_result = Bool(DEFAULT_OUTPUT_RESULT, help="Output query result to stdout").tag(config=True)
    notify_result = Bool(DEFAULT_NOTIFY_RESULT, help="Notify query result to stdout").tag(config=True)

    def __init__(self, shell):
        # access shell environment
        self.shell = shell
        self.caller = None
        self.notify_obj = Notify(shell)
        self.shell.configurables.append(self)
        Configurable.__init__(self, config=shell.config)
        Magics.__init__(self, shell=shell)


    @validate('output_result')
    def _validate_output_result(self, proposal):
        try:
            return bool(proposal['value'])
        except:
            raise TraitError('output_result: "{}" is not accepted. Value must be boolean'.format(proposal['value']))

    @validate('conn_object_name')
    def _validate_conn_object(self, proposal):
        return conn_val.validate_conn_object(proposal['value'], self.shell)

    @observe('conn_object_name')
    def _assign_connection(self, change):
        new_conn_object_name = change['new']
        conn_object = self.shell.user_global_ns[new_conn_object_name]
        self.caller = conn_val.read_connection(conn_object)


    def _parse_read_sql_args(self, line_string):
        ap = utils.create_flag_parser()
        opts = ap.parse_args(line_string.split())
        return {'table_name': opts.table_name, 'display': opts.display, 'notify': opts.notify,
                'async': opts.async, 'force_caller': opts.connection}



    def _read_sql_engine(self, sql, options):
        option_keys = ['table_name', 'display', 'notify', 'force_caller', 'async']
        table_name, show_output, notify_result, force_caller, async = [options[k] for k in option_keys]
        self.shell.all_ns_refs[0][table_name] = 'QUERY RUNNING'

        if force_caller:
            conn_val.validate_conn_object(force_caller, self.shell)
            force_caller_obj = self.shell.user_global_ns[force_caller]
            caller = conn_val.read_connection(force_caller_obj)
        else:
            caller = self.caller
        result, del_time = engine.time_and_run_query(caller, sql)

        if table_name:
            # add to iPython namespace
            #TODO: self.shell.user_ns.update({result_var: result})
            self.shell.all_ns_refs[0][table_name] = result
        query_has_result = not isinstance(result, EmptyResult)
        if show_output and query_has_result:
            self.shell.displayhook(result)
        if notify_result:
            self.notify_obj.notify_complete(del_time, table_name, result.shape)

    def _execute_sqls(self, sqls, options):
        for i, s in enumerate(sqls, start=1):
            self._read_sql_engine(s, options)

    @needs_local_scope
    @cell_magic
    # @line_magic
    def read_sql(self, line, cell='', local_ns={}):
        #TODO: Fix namespace/scope issue
        # save globals and locals so they can be referenced in bind vars
        user_ns = self.shell.user_ns.copy()
        user_ns.update(local_ns)

        options = self._parse_read_sql_args(line)
        sql = cell.format(**self.shell.all_ns_refs[0])
        options['notify'] = self.notify_result ^ options['notify']
        statements = [s for s in sqlparse.split(sql) if s]  # exclude blank statements
        if options['async']:
            options['display'] = False ^ options['display']  # default to False, unless user provides flag
            t = threading.Thread(target=self._execute_sqls, args=[statements, options])
            t.start()
        else:
            options['display'] = self.output_result ^ options['display']
            self._execute_sqls(statements, options)


def load_ipython_extension(ip):
    """Load the extension in IPython."""
    utils.add_syntax_coloring()
    ip.register_magics(SQLConn)

def unload_ipython_extension(ip):
    if 'SQLConn' in ip.magics_manager.registry:
        del ip.magics_manager.registry['SQLConn']
    if 'SQLConn' in ip.config:
        del ip.config['SQLConn']