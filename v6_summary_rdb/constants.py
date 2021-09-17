from v6_summary_rdb.sql_functions import *

PGDATABASE = "PGDATABASE"

COUNT_FUNCTION = "count"
AVG_FUNCTION = "avg"
MAX_FUNCTION = "max"
MIN_FUNCTION = "min"
SUM_FUNCTION = "sum"
STD_SAMP_FUNCTION = "stddev_samp"
POOLED_STD_FUNCTION = "pooled_std"
COUNT_NULL = "count_null"

HISTOGRAM = "histogram"
BOXPLOT = "boxplot"
QUARTILES = "quartiles"

VARIABLE = "variable"
TABLE = "table"
FUNCTIONS = "functions"
REQUIRED_FUNCTIONS = "required_functions"
REQUIRED_METHODS = "required_methods"
METHOD = "METHOD"

ERROR = "error"

NAME = "NAME"
CALL = "CALL"
FETCH = "FETCH"
FETCH_ONE = "FETCH_ONE"
FETCH_ALL = "FETCH_ALL"

FUNCTION_MAPPING = {
    COUNT_FUNCTION: {
        FUNCTIONS: [COUNT_FUNCTION]
    },
    AVG_FUNCTION: {
        FUNCTIONS: [SUM_FUNCTION, COUNT_FUNCTION]
    },
    MAX_FUNCTION: {
        FUNCTIONS: [MAX_FUNCTION]
    },
    MIN_FUNCTION: {
        FUNCTIONS: [MIN_FUNCTION]
    },
    SUM_FUNCTION: {
        FUNCTIONS: [SUM_FUNCTION]
    },
    POOLED_STD_FUNCTION: {
        FUNCTIONS: [STD_SAMP_FUNCTION]
    },
    HISTOGRAM: {
        METHOD: {
            NAME: HISTOGRAM,
            CALL: histogram,
            FETCH: FETCH_ALL
        },
    },
    BOXPLOT: {
        FUNCTIONS: [MAX_FUNCTION, MIN_FUNCTION],
        METHOD: {
            NAME: QUARTILES,
            CALL: quartiles,
            FETCH: FETCH_ONE
        }
    },
    COUNT_NULL: {
        METHOD: {
            NAME: COUNT_NULL,
            CALL: count_null,
            FETCH: FETCH_ONE
        }
    }
}
