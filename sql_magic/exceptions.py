class NoReturnValueResult(Exception):
    pass

class AsyncError(Exception):
    pass

class EmptyResult(object):
    shape = None

    def __str__(self):
        return ''