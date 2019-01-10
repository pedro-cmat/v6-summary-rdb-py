import json
import os

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
"""The input.txt, output.txt and 'DATABASE_URI' are all mounted files.
""""

# read input from the mounted inputfile
input_ = fileIO.load_json_from_file("input.txt")

# determine function from input
method = function_name_mapper(input_.get("method","summarize"))

# read dataframe
dataframe = fileIO.load_dataframe(os.environ['DATABASE_URI'])

# call function
output = method(dataframe, input_.get("column_names",[]))

# write output to mounted output file
fileIO.write_output_to_file("output.txt", json.dumps(output))
