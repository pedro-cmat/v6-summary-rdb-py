from v6_summary_rdb.constants import *

def count(results):
    """ Calculates the global count.
    """
    return sum([result[COUNT_FUNCTION] for result in results])

def maximum(results):
    """ Calculates the global maximum.
    """
    return max([result[MAX_FUNCTION] for result in results])

def minimum(results):
    """ Calculates the global minimum.
    """
    return min([result[MIN_FUNCTION] for result in results])

def average(results):
    """ Calculates de global average.
    """
    count = 0
    total = 0
    for result in results:
        count += result[COUNT_FUNCTION]
        total += result[SUM_FUNCTION]
    return total/count
