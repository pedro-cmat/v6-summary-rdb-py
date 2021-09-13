""" Utility functions.
"""

def run_sql(db_client, sql_statement, parameters=None, fetch_all=False):
    # Execute the sql query and retrieve the results
    db_client.execute(sql_statement, parameters)
    if fetch_all:
        return db_client.fetchall()
    else:
        return db_client.fetchone()
