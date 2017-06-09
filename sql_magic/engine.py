import time
# def _read_sql_engine(sql, options, shell):
#     table_name, show_output, notify_result, force_caller, async = [options[k] for k in ['table_name', 'display',
#                                                                                         'notify', 'force_caller',
#                                                                                         'async']]
#     shell.all_ns_refs[0][table_name] = 'QUERY RUNNING'
#     try:
#         if force_caller:
#             self._validate_conn_object_name({'value': force_caller})
#             caller = self._read_connection(force_caller)
#         else:
#             caller = self.caller
#         # TODO: if force caller, use that
#         result, del_time = self._time_and_run_query(caller, sql)
#     except Exception as e:  # pandas' read_sql/sqlalchemy complains if no result
#         print(str(e))
#         no_result_error = (str(e) == 'This result object does not return rows. It has been closed automatically.')
#         if not no_result_error:
#             raise Exception(e)
#     if table_name:
#         # add to iPython namespace
#         # TODO: self.shell.user_ns.update({result_var: result})
#         self.shell.all_ns_refs[0][table_name] = result
#     query_has_result = not isinstance(result, EmptyResult)
#     if show_output and query_has_result:
#         self.shell.displayhook(result)
#     if notify_result:
#         self.notify_obj.notify_complete(del_time, table_name, result.shape)

# def _read_sql_engine(sql, options, shell):
#     option_keys = ['table_name', 'display', 'notify', 'force_caller', 'async']
#     table_name, show_output, notify_result, force_caller, async = [options[k] for k in option_keys]
#     self.shell.all_ns_refs[0][table_name] = 'QUERY RUNNING'
#     if force_caller:
#         conn_val.validate_conn_object(force_caller, self.shell)
#         caller = self._read_connection(force_caller)
#     else:
#         caller = self.caller
#     result, del_time = engine.time_and_run_query(caller, sql)
#
#     if table_name:
#         # add to iPython namespace
#         #TODO: self.shell.user_ns.update({result_var: result})
#         self.shell.all_ns_refs[0][table_name] = result
#     query_has_result = not isinstance(result, EmptyResult)
#     if show_output and query_has_result:
#         self.shell.displayhook(result)
#     if notify_result:
#         self.notify_obj.notify_complete(del_time, table_name, result.shape)

def time_and_run_query(caller, sql):
    pretty_start_time = time.strftime('%I:%M:%S %p %Z')
    print('Query started at {}'.format(pretty_start_time))
    start_time = time.time()
    result = caller(sql)
    end_time = time.time()
    del_time = (end_time-start_time)/60.
    print('Query executed in {:2.2f} m'.format(del_time))
    return result, del_time

# def _execute_sqls(sqls, options):
#     for i, s in enumerate(sqls, start=1):
#         self._read_sql_engine(s, options)