import json
import pandas

def load_json_from_file(filename: str):
    """"""
    with open(filename) as fp:
        json_data = json.loads(fp.read())
    return json_data

def load_dataframe(filename: str, dtype, sep=",", decimal="."):
    return pandas.read_csv(filename, sep=sep,decimal=decimal,dtype=dtype)

def write_output_to_file(filename: str, output):
    """"""
    with open(filename, 'w') as fp:
        fp.write(output)