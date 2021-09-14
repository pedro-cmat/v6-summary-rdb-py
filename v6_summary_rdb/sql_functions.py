BIN_WIDTH = "BIN_WIDTH"

def histogram(variable, table, arguments):
    """ Create the SQL statement to obtain the necessary information
        for an histogram.
    """
    width = None
    if BIN_WIDTH in arguments:
        width = arguments[BIN_WIDTH]
        if not isinstance(width, int) or width <= 1:
            raise Exception("Invalid bin width provided, value must be a integer superior to 1.")
    else:
        raise Exception("Histogram requested but the bin width (argument: BIN_WIDTH) must be provided!")

    return f"""SELECT floor("{variable}"/{width})*{width} as bins, COUNT(*) 
        FROM {table} GROUP BY 1 ORDER BY 1"""
