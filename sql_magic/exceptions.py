class NoReturnValueResult(Exception):
    pass


class AsyncError(Exception):
    pass


class EmptyResult(object):
    shape = None  # simulate object dimension (pandas)

    def __str__(self):
        return ''