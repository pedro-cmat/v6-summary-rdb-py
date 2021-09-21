""" Utility functions.
"""

from v6_summary_rdb.constants import ERROR

def run_sql(db_client, sql_statement, parameters=None, fetch_all=False):
    """ Execute the sql query and retrieve the results
    """
    db_client.execute(sql_statement, parameters)
    if fetch_all:
        return db_client.fetchall()
    else:
        return db_client.fetchone()

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
