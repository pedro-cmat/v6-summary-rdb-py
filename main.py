import json
import os
import sys

from summary import summary, info, warn

# read input from the mounted inputfile
info("Reading input")
with open("app/input.txt") as fp:
    input_ = json.loads(fp.read())

# determine function from input, summarize is used by default.
# and get the args and kwargs input for this function
method_name = input_.get("method","summary")
method = {
    "summary": summary
}.get(method_name)
if not method:
    warn(f"method name={method_name} not found!\n")
    exit()

args = input_.get("args", [])
kwargs = input_.get("kwargs", {})

# call function
output = method(*args, **kwargs)

# write output to mounted output file
info("Writing output")
with open("app/output.txt", 'w') as fp:
    fp.write(json.dumps(output))
