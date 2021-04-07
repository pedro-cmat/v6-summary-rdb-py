import sys
import pandas
import time
import json

from vantage6.tools.util import warn, info

from v6_summary_rdb.aggregators import *
from v6_summary_rdb.constants import *

AGGREGATORS = {
    COUNT_FUNCTION: count,
    MAX_FUNCTION: maximum,
    MIN_FUNCTION: minimum,
    AVG_FUNCTION: average
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
        A dictionary containing summary statistics for all the columns of the
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
                    column[FUNCTIONS] = list(AGGREGATORS.keys())
            # Check if it supports all functions
            unsupported_functions = [function for function in column[FUNCTIONS] if function not in AGGREGATORS.keys()]
            if len(unsupported_functions) > 0:
                warn(f"Unsupported functions: {', '.join(unsupported_functions)}")
                return None

            column[REQUIRED_FUNCTIONS] = set([
                r_function for function in column[FUNCTIONS] for r_function in FUNCTION_MAPPING[function]])
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
    summary = {}
    for column in columns:
        # construct the sql statement
        sql_functions = f"COUNT({column[VARIABLE]})"
        for function in column[REQUIRED_FUNCTIONS]:
            if function.upper() not in sql_functions:
                sql_functions += f", {function.upper()}({column[VARIABLE]})"
        sql_statement = f"SELECT {sql_functions} FROM {column[TABLE].upper()} WHERE {column[VARIABLE]} IS NOT NULL"

        # execute the sql query and retrieve the results
        try:
            db_client.execute(sql_statement)
            result = db_client.fetchone()
        except Exception as error:
            warn("Error while executing the sql query.")
            return {
                ERROR: str(error)
            }

        # parse the results
        summary[column[VARIABLE]] = {}
        for i, function in enumerate(column[REQUIRED_FUNCTIONS]):
            summary[column[VARIABLE]][function] = result[i]

    return summary
