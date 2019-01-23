import os
import sys
import pandas

# loggers
info = lambda msg: sys.stdout.write("info > "+msg+"\n")
warn = lambda msg: sys.stdout.write("warn > "+msg+"\n")

def summary(columns, decimal, seperator):

    # create series from input column names
    columns_series = pandas.Series(data=columns)

    # create pandas dataframe from csv-file
    info("Reading database-file")
    dataframe = pandas.read_csv(
        os.environ['DATABASE_URI'], 
        sep=seperator,
        decimal=decimal,
        dtype=columns
    )

    # compare column names from dataset to the input column names
    info("Checking column-names")
    column_names_correct = list(dataframe.keys()) == list(columns_series.keys())
    if not column_names_correct:
        warn("Column names do not match. Exiting.")
        return {"column_names_correct": column_names_correct}

    # count the number of rows in the dataset
    # TODO should the minimal row-count be controlled by the Node?
    info("Counting number of rows")
    number_of_rows = len(dataframe)
    if number_of_rows < 10:
        warn("Dataset has less than 10 rows. Exiting.")
        return {
            "column_names_correct": column_names_correct,
            "number_of_rows": number_of_rows
        }

    # min, max, median, average, Q1, Q3, missing_values
    columns = {}
    numeric_colums = columns_series.loc[columns_series.isin(['Int64','float64'])]
    for column_name in numeric_colums.keys():
        info(f"Numerical column={column_name} is processed")
        column_values = dataframe[column_name]
        q1, median, q3 = column_values.quantile([0.25,0.5,0.75]).values
        columns[column_name] = {
            "min": int(column_values.min()),
            "q1": int(q1),
            "median": int(median),
            "mean": int(column_values.mean()),
            "q3": int(q3),
            "max": int(column_values.max()),
            "nan": int(column_values.isna().sum())
        }
        
    # return the categories in categorial columns
    categoral_colums = columns_series.loc[columns_series.isin(['category'])]
    for column_name in categoral_colums.keys():
        info(f"Categorial column={column_name} is processed")
        columns[column_name] = dataframe[column_name].value_counts().to_dict()

    return {
        "column_names_correct": column_names_correct,
        "number_of_rows": number_of_rows,
        "statistics": columns
    }
