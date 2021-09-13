BIN_WIDTH = "BIN_WIDTH"

def histogram(variable, table, arguments):
    """ Create the SQL statement to obtain the necessary information
        for an histogram.
    """
    width = arguments[BIN_WIDTH]
    return f"""SELECT floor("{variable}"*{width})/{width} as bins, COUNT(*) 
        FROM {table} GROUP BY 1 ORDER BY 1"""
