import sys
import pandas
import time
import json

from vantage6.tools.util import warn, info

from v6_summary_rdb.aggregators import *
from v6_summary_rdb.constants import *
from v6_summary_rdb.utils import run_sql

DEFAULT_FUNCTIONS = [
    MAX_FUNCTION, MIN_FUNCTION, AVG_FUNCTION, POOLED_STD_FUNCTION
]

AGGREGATORS = {
    MAX_FUNCTION: maximum,
    MIN_FUNCTION: minimum,
    AVG_FUNCTION: average,
    POOLED_STD_FUNCTION: pooled_std,
    HISTOGRAM: histogram_aggregator,
    BOXPLOT: boxplot,
    COUNT_FUNCTION: count,
    COUNT_NULL: sum_null,
    COUNT_DISCRETE: count_discrete,
}

def master(client, db_client, columns, functions):
    """
    Master algorithm to compute a summary of the federated datasets.

    Parameters
    ----------
    client : ContainerClient
        Interface to the central server. This is supplied by the wrapper.
    db_client : DBClient
        The database client.
    columns : List
        List containing the columns and information needed.

    Returns
    -------
    Dict
        A dictionary containing summary statistics for the chosen columns of the
        dataset.
    """
    # Validating the input
    info("Validating the input arguments")
    if type(columns) == list:
        for column in columns:
            if not all([parameter in column for parameter in [VARIABLE, TABLE]]):
                warn("Missing information in the input argument")
                return None                
            # check which functions to run
            if FUNCTIONS not in column:
                if functions:
                    column[FUNCTIONS] = functions
                else:
                    column[FUNCTIONS] = DEFAULT_FUNCTIONS
            # Check if it supports all functions
            unsupported_functions = [function for function in column[FUNCTIONS] if function not in AGGREGATORS.keys()]
            if len(unsupported_functions) > 0:
                warn(f"Unsupported functions: {', '.join(unsupported_functions)}")
                return None
            column[REQUIRED_FUNCTIONS] = set()
            column[REQUIRED_METHODS] = []
            for function in column[FUNCTIONS]:
                if FUNCTIONS in FUNCTION_MAPPING[function]:
                    column[REQUIRED_FUNCTIONS].update(FUNCTION_MAPPING[function][FUNCTIONS]) 
                if METHOD in FUNCTION_MAPPING[function]:
                    column[REQUIRED_METHODS].append(FUNCTION_MAPPING[function][METHOD])
    else:
        warn("Invalid format for the input argument")
        return None


    # define the input for the summary algorithm
    info("Defining input parameters")
    input_ = {
        "method": "summary",
        "args": [],
        "kwargs": {
            "columns": columns
        }
    }

    # obtain organizations that are within my collaboration
    info("Obtaining the organizations in the collaboration")
    organizations = client.get_organizations_in_my_collaboration()
    ids = [organization.get("id") for organization in organizations]

    # collaboration and image is stored in the key, so we do not need
    # to specify these
    info("Creating node tasks")
    task = client.create_new_task(
        input_,
        organization_ids=ids
    )

    # wait for all results
    # TODO subscribe to websocket, to avoid polling
    task_id = task.get("id")
    task = client.request(f"task/{task_id}")
    while not task.get("complete"):
        task = client.request(f"task/{task_id}")
        info("Waiting for results")
        time.sleep(1)

    info("Obtaining results")
    results = client.get_results(task_id=task.get("id"))

    info("Check if any exception occurred")
    if any([ERROR in result for result in results]):
        warn("Encountered an error, please review the parameters")
        return [result[ERROR] for result in results if ERROR in result]

    # process the output
    info("Process the node results")
    summary = {}

    for column in columns:
        summary[column[VARIABLE]] = {}
        nodes_summary = [result[column[VARIABLE]] for result in results]
        for function in column[FUNCTIONS]:
            summary[column[VARIABLE]][function] = AGGREGATORS[function](nodes_summary)

    return summary

def RPC_summary(db_client, columns):
    """
    Computes a summary of the requested columns

    Parameters
    ----------
    db_client : DBClient
        The database client.
    columns : List
        List containing the columns and information needed.

    Returns
    -------
    Dict
        A Dict containing a summary for the columns requested.
    """
    info("Summary node method.")
    summary = {}
    for column in columns:
        if REQUIRED_FUNCTIONS in column:
            # construct the sql statement
            variable = f'"{column[VARIABLE]}"'
            sql_functions = ''
            for function in column[REQUIRED_FUNCTIONS]:
                if function.upper() not in sql_functions:
                    sql_functions += f"{' ,' if sql_functions else ''}{function.upper()}({variable})"
            sql_statement = f"SELECT {sql_functions} FROM {column[TABLE].upper()} WHERE {variable} IS NOT NULL"
            # execute the sql query and retrieve the results
            try:
                result = run_sql(db_client, sql_statement)
            except Exception as error:
                warn("Error while executing the sql query.")
                return {
                    ERROR: str(error)
                }
            # parse the results
            summary[column[VARIABLE]] = {}
            for i, function in enumerate(column[REQUIRED_FUNCTIONS]):
                summary[column[VARIABLE]][function] = result[i]

        if REQUIRED_METHODS in column:
            for method in column[REQUIRED_METHODS]:
                try:
                    sql_statement = method[CALL](
                        column[VARIABLE], column[TABLE].upper(), column)
                    summary[column[VARIABLE]][method[NAME]] = run_sql(
                        db_client, sql_statement, fetch_all = method[FETCH]==FETCH_ALL
                    )
                except Exception as error:
                    warn("Error while executing the sql query.")
                    return {
                        ERROR: str(error)
                    }
    return summary
