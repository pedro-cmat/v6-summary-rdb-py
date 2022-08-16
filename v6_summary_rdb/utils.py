""" Utility functions.
"""
from decimal import Decimal
import os

from v6_summary_rdb.constants import *

def run_sql(db_client, sql_statement, parameters=None, fetch_all=False, convert_type=True):
    """ Execute the sql query and retrieve the results
    """
    db_client.execute(sql_statement, parameters)
    results = db_client.fetchall() if fetch_all else db_client.fetchone()
    return parse_decimal_to_float(results) if convert_type else results

def parse_error(error_message):
    """ Parse an error message.
    """
    return {
        ERROR: error_message 
    }

def check_keys_in_dict(keys, map):
    """ Check if all keys are present in a dictionary.
    """
    return all([key in map for key in keys])

def compare_with_minimum(value):
    """ Compare the value with the minimum value allowed.
    """
    count_minimum = int(os.getenv(COUNT_MINIMUM) or COUNT_MINIMUM_DEFAULT)
    return value if value > count_minimum else f"< {count_minimum}"

def parse_sql_condition(sql_condition, where_condition=False):
    """ Parse the sql condition to insert in another sql statement.
    """
    return f"""{"WHERE" if where_condition else "AND"} {sql_condition}""" \
        if sql_condition else ""

def parse_decimal_to_float(values):
    """ Parse decimals to float in a list.
    """
    parsed_list = []
    for item in values:
        if isinstance(item, list) or isinstance(item, tuple):
            parsed_list.append(parse_decimal_to_float(item))
        elif isinstance(item, Decimal):
            parsed_list.append(float(item))
        else:
            parsed_list.append(item)
    return parsed_list
