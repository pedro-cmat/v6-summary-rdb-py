import os

from v6_summary_rdb.constants import *
from v6_summary_rdb.utils import parse_sql_condition

def histogram(variable, table, sql_condition, arguments):
    """ Create the SQL statement to obtain the necessary information
        for an histogram.
    """
    width = None
    if BIN_WIDTH in arguments:
        width = arguments[BIN_WIDTH]
        if not isinstance(width, int) or width < int(os.getenv(BIN_WIDTH_MINIMUM) or BIN_WIDTH_MINIMUM_DEFAULT):
            raise Exception("Invalid bin width provided, value must be a integer superior to 1.")
    else:
        raise Exception("Histogram requested but the bin width (argument: BIN_WIDTH) must be provided!")

    return f"""SELECT floor("{variable}"/{width})*{width} as bins, COUNT(*) 
        FROM {table} {parse_sql_condition(sql_condition, where_condition=True)} GROUP BY 1 ORDER BY 1;"""

def quartiles(variable, table, sql_condition, arguments):
    """ Create the SQL statement to obtain the 25th, 50th, and 75th 
        quartiles for a variable.
    """
    iqr_threshold = arguments.get(IQR_THRESHOLD) or IQR_THRESHOLD_DEFAULT
    return f"""with percentiles AS (SELECT current_database() as db,
        percentile_cont(0.25) within group (order by "{variable}" asc) as q1,
        percentile_cont(0.50) within group (order by "{variable}" asc) as q2,
        percentile_cont(0.75) within group (order by "{variable}" asc) as q3
        FROM {table} {parse_sql_condition(sql_condition, where_condition=True)})
        SELECT *, q1 - (q3 - q1) * {iqr_threshold} AS lower_bound,
        q3 + (q3 - q1) * {iqr_threshold} AS upper_bound,
        (SELECT count("{variable}") FROM {table} WHERE 
            "{variable}" < q1 - (q3 - q1) * {iqr_threshold} 
            {parse_sql_condition(sql_condition)}) AS lower_outliers,
        (SELECT count("{variable}") FROM {table} WHERE 
            "{variable}" > q3 + (q3 - q1) * {iqr_threshold} 
            {parse_sql_condition(sql_condition)}) AS upper_outliers
        FROM percentiles;
    """

def count_null(variable, table, sql_condition, arguments):
    """ Create the SQL statment to count the null values.
    """
    return f"""SELECT count(*) FROM {table} WHERE "{variable}" IS NULL
        {parse_sql_condition(sql_condition)};"""

def count_discrete_values(variable, table, sql_condition, arguments):
    """ Count the discrete values.
    """
    return f"""SELECT "{variable}", count(*) FROM {table} 
        {parse_sql_condition(sql_condition, where_condition=True)} GROUP BY "{variable}";"""

def cohort_count(id_column, definition, table):
    """ Count the number of persons in a possible cohort.
    """
    sql_condition = ''
    for component in definition:
        sql_condition += f' AND' if sql_condition else ''
        sql_condition += f' "{component[VARIABLE]}" {component[OPERATOR]} {component[VALUE]}'
    return (f"""SELECT current_database() as db, COUNT("{id_column or "*"}") 
        FROM {table} WHERE {sql_condition}""",
        sql_condition,
    )
