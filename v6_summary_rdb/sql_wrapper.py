import os

from v6_summary_rdb.constants import *
from v6_summary_rdb.utils import run_sql, compare_with_minimum, parse_sql_condition
from v6_summary_rdb.sql_functions import cohort_count

def cohort_finder(cohort, db_client):
    """ Retrieve the results for the cohort finder option
    """
    sql_statement, sql_condition = cohort_count(
        ID_COLUMN in cohort and cohort[ID_COLUMN],
        cohort[COHORT_DEFINITION],
        cohort[TABLE],
    )
    # If the total count for the cohort is below the accepted threshold
    # then the information won't be sent to the mater node
    result = run_sql(db_client, sql_statement)
    return ((result[0], compare_with_minimum(result[1])), sql_condition)

def summary_results(columns, sql_condition, db_client):
    """ Retrieve the summary results for the requested functions
    """
    summary = {}
    sql_functions = None
    for column in columns:
        # validate the number of records available prior to obtaining any
        # summary statistics
        variable = f'"{column[VARIABLE]}"'
        table = column[TABLE].upper()
        sql_statement = f"""SELECT current_database(), COUNT({variable}) FROM {table} 
            WHERE {variable} IS NOT NULL {parse_sql_condition(sql_condition)};"""
        result = run_sql(db_client, sql_statement)
        summary[column[VARIABLE]] = {}
        if int(result[1]) >= int(os.getenv(TABLE_MINIMUM) or TABLE_MINIMUM_DEFAULT):
            if REQUIRED_FUNCTIONS in column:
                # construct the sql statement
                sql_functions = ""
                for function in column[REQUIRED_FUNCTIONS]:
                    if function.upper() not in sql_functions:
                        sql_functions += f"{' ,' if sql_functions else ''}{function.upper()}({variable})"
                sql_statement = f"""SELECT {sql_functions} FROM {table} WHERE 
                    {variable} IS NOT NULL {parse_sql_condition(sql_condition)};"""
                # execute the sql query and retrieve the results
                result = run_sql(db_client, sql_statement)
                # parse the results
                for i, function in enumerate(column[REQUIRED_FUNCTIONS]):
                    summary[column[VARIABLE]][function] = result[i]

            if REQUIRED_METHODS in column:
                for method in column[REQUIRED_METHODS]:
                    sql_statement = method[CALL](
                        column[VARIABLE], column[TABLE].upper(), sql_condition, column)
                    summary[column[VARIABLE]][method[NAME]] = run_sql(
                        db_client, sql_statement, fetch_all = method[FETCH]==FETCH_ALL
                    )
        else:
           summary[column[VARIABLE]][WARNING] = f"Not enough records in database {result[0]}" + \
               " to execute the summary statistics" 
    return summary