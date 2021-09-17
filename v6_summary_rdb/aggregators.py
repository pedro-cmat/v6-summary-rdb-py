import math

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

def pooled_std(results):
    """ Calculated the pooled standard deviation. 
    """
    sample_size = 0
    weighted_std = 0
    for result in results:
        sample_size += result[COUNT_FUNCTION]
        weighted_std += (result[COUNT_FUNCTION] - 1) * (result[STD_SAMP_FUNCTION] ** 2)
    return math.sqrt(weighted_std / (sample_size - len(results)))

def histogram_aggregator(results):
    """ Aggregate the histograms. 
    """
    histogram = {}
    for result in results:
        for bins in result[HISTOGRAM]:
            if bins[0] not in histogram:
                histogram[bins[0]] = 0
            histogram[bins[0]] += int(bins[1])
    return histogram

def boxplot(results):
    """ Aggregate the quartiles by node providing the information to
        build a boxplot.
    """
    aggregated_quartiles = {}
    for result in results:
        quartiles = result[QUARTILES]
        aggregated_quartiles[quartiles[0]] = {
            MIN_FUNCTION: result[MIN_FUNCTION],
            "q1": quartiles[1],
            "median": quartiles[2],
            "q3": quartiles[3],
            MAX_FUNCTION: result[MAX_FUNCTION]
        }
    return aggregated_quartiles

def sum_null(results):
    """ Calculate the total number of null values.
    """
    return sum([result[COUNT_NULL][0] for result in results])
