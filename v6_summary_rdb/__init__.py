import time

from vantage6.tools.util import warn, info

from v6_summary_rdb.aggregators import cohort_aggregator
from v6_summary_rdb.constants import *
from v6_summary_rdb.mapping import AGGREGATORS, FUNCTION_MAPPING
from v6_summary_rdb.sql_wrapper import cohort_finder, summary_results
from v6_summary_rdb.utils import *

def master(client, db_client, columns = [], functions = None, cohort = None, org_ids = None):
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
        if not check_keys_in_dict([COHORT_DEFINITION, TABLE, ID_COLUMN], cohort) or \
            len(cohort[COHORT_DEFINITION]) < 1 or \
            not all([check_keys_in_dict([VARIABLE, OPERATOR, VALUE], definition) for definition in cohort[COHORT_DEFINITION]]):
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
    ids = [organization.get("id") for organization in organizations if \
        not org_ids or organization.get("id") in org_ids]
    info(f"Sending the algorithm to the following organizations: {' ,'.join(str(id) for id in ids)}")

    # collaboration and image is stored in the key, so we do not need
    # to specify these
    info("Creating node tasks")
    task = client.create_new_task(
        input_,
        organization_ids=ids
    )

    # wait for all results
    task_id = task.get("id")
    task = client.request(f"task/{task_id}")
    while not task.get("complete"):
        task = client.request(f"task/{task_id}")
        info("Waiting for results")
        time.sleep(5)

    info("Obtaining results")
    results = client.get_results(task_id=task.get("id"))

    info("Check if any exception occurred")
    if any([ERROR in result for result in results]):
        warn("Encountered an error, please review the parameters")
        return [result[ERROR] for result in results if ERROR in result]

    # process the output
    info("Process the node results")
    try:
        summary = {}
        cohort_error = False
        if cohort:
            warnings = [result[COHORT][WARNING] for result in results if WARNING in result[COHORT]]
            if len(warnings) > 0:
                summary[COHORT] = warnings
                cohort_error = True
            else:
                summary[COHORT] = cohort_aggregator([result[COHORT] for result in results])

        if not cohort or not cohort_error:
            for column in columns:
                variable_name = column[VARIABLE]
                summary[variable_name] = {}
                warnings = [result[variable_name][WARNING] for result in results if WARNING in result[variable_name]]
                if len(warnings) > 0:
                    summary[variable_name] = warnings
                else:
                    nodes_summary = [result[variable_name] for result in results]
                    for function in column[FUNCTIONS]:
                        summary[variable_name][function] = AGGREGATORS[function](nodes_summary)
        return summary
    except Exception as error:
            return parse_error(f"Error while aggregating the results: {str(error)}")

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
    # Execute the necessary SQL queries and aggregate the results
    summary = {}
    # Process the cohort if included in the request
    sql_condition = None
    if cohort:
        try:
            (summary[COHORT], sql_condition) = cohort_finder(cohort, db_client)
        except Exception as error:
            return parse_error(f"Error while executing the sql query for the cohort analysis {str(error)}")

    if not cohort or sql_condition:
        try:
            summary.update(summary_results(columns, sql_condition, db_client))
        except Exception as error:
            return parse_error(f"Error while executing the sql query for the summary: {str(error)}")

    return summary
