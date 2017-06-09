import time

def time_and_run_query(caller, sql):
    pretty_start_time = time.strftime('%I:%M:%S %p %Z')
    print('Query started at {}'.format(pretty_start_time))
    start_time = time.time()
    result = caller(sql)
    end_time = time.time()
    del_time = (end_time-start_time)/60.
    print('Query executed in {:2.2f} m'.format(del_time))
    return result, del_time
