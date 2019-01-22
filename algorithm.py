import sys

def summarize_dataframe(dataframe, columns_series):

    # retrieve column names from the dataset
    column_names = list(dataframe.keys())

    # compare column names from dataset to the input column names
    column_names_correct = column_names == list(columns_series.keys())
    if not column_names_correct:
        sys.stdout.write("Column names do not match. Algorithm terminated.")
        return {
            "column_names_correct": column_names_correct,
        }
    # print(f"column_names_correct={column_names_correct}")

    # count the number of rows in the dataset
    # TODO should the minimal row-count be controlled by the Node?
    number_of_rows = len(dataframe)
    if number_of_rows < 10:
        sys.stdout.write("Dataset has less than 10 rows. Algorithm terminated")
        return {
            "column_names_correct": column_names_correct,
            "number_of_rows": number_of_rows
        }
    sys.stdout.write(f"number_of_rows={number_of_rows}")

    ### statistic summary 
    columns = {}

    # numeric columns report: min, max, median, average, Q1, Q3, missing_values
    numeric_colums = columns_series.loc[columns_series.isin(['Int64','float64'])]
    for column_name in numeric_colums.keys():
        column_values = dataframe[column_name]
        q1, median, q3 = column_values.quantile([0.25,0.5,0.75]).values
        mean = column_values.mean()
        minimum = column_values.min()
        maximum = column_values.max()
        nan = column_values.isna().sum()
        columns[column_name] = {
            "min": int(minimum),
            "q1": int(q1),
            "median": int(median),
            "mean": int(mean),
            "q3": int(q3),
            "max": int(maximum),
            "nan": int(nan)
        }
        sys.stdout.write(f"Column {column_name} processed")

    # return the categories in categorial columns
    categoral_colums = columns_series.loc[columns_series.isin(['category'])]
    for column_name in categoral_colums.keys():
        columns[column_name] = dataframe[column_name].value_counts().to_dict()
        sys.stdout.write(f"Column {column_name} processed")

    return {
        "column_names_correct": column_names_correct,
        "number_of_rows": number_of_rows,
        "statistics": columns
    }
