import json
import os

import pandas

import fileIO
import algorithm



def function_name_mapper(function_name: str):
    """Maps the input method name (from the input file) to actual 
    functions. If input is not recogn"""

    function = {
        "summarize": algorithm.summarize_dataframe
    }.get(function_name, None)

    return function


########################################################################

# read input from the mounted inputfile
input_ = fileIO.load_json_from_file("input.txt")
columns_series = pandas.Series(data=input_.get("columns"))

# determine function from input
method = function_name_mapper(input_.get("method","summarize"))

# read dataframe
dataframe = fileIO.load_dataframe(
    os.environ['DATABASE_URI'],
    dtype=input_.get("columns")
)

# call function
output = method(dataframe, columns_series)

# write output to mounted output file
fileIO.write_output_to_file("output.txt", json.dumps(output))
