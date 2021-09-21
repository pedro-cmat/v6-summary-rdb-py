import time

from vantage6.tools.util import warn, info

from v6_summary_rdb.aggregators import cohort_aggregator
from v6_summary_rdb.constants import *
from v6_summary_rdb.mapping import AGGREGATORS, FUNCTION_MAPPING
from v6_summary_rdb.sql_functions import cohort_count
from v6_summary_rdb.utils import run_sql, parse_error

def master(client, db_client, columns = [], functions = None, cohort = None):
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
    cohort: Dict
        Information to identify the number of persons in a specific cohort.

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
                return parse_error("Missing information in the input argument")                
            # check which functions to run
            if FUNCTIONS not in column:
                if functions:
                    column[FUNCTIONS] = functions
                else:
                    column[FUNCTIONS] = DEFAULT_FUNCTIONS
            # Check if it supports all functions
            unsupported_functions = [function for function in column[FUNCTIONS] if function not in AGGREGATORS.keys()]
            if len(unsupported_functions) > 0:
                return parse_error(f"Unsupported functions: {', '.join(unsupported_functions)}")
            column[REQUIRED_FUNCTIONS] = set()
            column[REQUIRED_METHODS] = []
            for function in column[FUNCTIONS]:
                if FUNCTIONS in FUNCTION_MAPPING[function]:
                    column[REQUIRED_FUNCTIONS].update(FUNCTION_MAPPING[function][FUNCTIONS]) 
                if METHOD in FUNCTION_MAPPING[function]:
                    column[REQUIRED_METHODS].append(FUNCTION_MAPPING[function][METHOD])
    else:
        return parse_error("Invalid format for the summary input argument")

    if cohort:
        if not all([info in cohort for info in [COHORT_DEFINITION, TABLE, ID_COLUMN]]) or \
            not all([VARIABLE in definition and OPERATOR in definition for definition in cohort[COHORT_DEFINITION]]):
            return parse_error("Invalid cohort definition for the cohort input argument")


    # define the input for the summary algorithm
    info("Defining input parameters")
    input_ = {
        "method": "summary",
        "args": [],
        "kwargs": {
            "columns": columns,
            "cohort": cohort
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

    if cohort:
        summary[COHORT] = cohort_aggregator([result[COHORT] for result in results])

    return summary

def RPC_summary(db_client, columns, cohort):
    """
    Computes a summary of the requested columns

    Parameters
    ----------
    db_client : DBClient
        The database client.
    columns : List
        List containing the columns and information needed.
    cohort: Dict
        Information to identify the number of persons in a specific cohort.

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
                warn("Error while executing the sql query for the summary (functions).")
                return parse_error(str(error))
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
                    warn(f"Error while executing the sql query for the summary method {method}.")
                    return parse_error(str(error))

    # Process the cohort if included in the request
    if cohort:
        try:
            sql_statement = cohort_count(
                ID_COLUMN in cohort and cohort[ID_COLUMN],
                cohort[COHORT_DEFINITION],
                cohort[TABLE],
            )
            summary[COHORT] = run_sql(db_client, sql_statement)
        except Exception as error:
            warn("Error while executing the sql query for the cohort analysis.")
            return parse_error(str(error))

    return summary
