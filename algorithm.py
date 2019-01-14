
def summarize_dataframe(dataframe, columns_series):

    # retrieve column names from the dataset
    column_names = list(dataframe.keys())

    # compare column names from dataset to the input column names
    column_names_correct = column_names == list(columns_series.keys())
    # print(f"column_names_correct={column_names_correct}")

    # count the number of rows in the dataset
    number_of_rows = len(dataframe)
    # print(f"number_of_rows={number_of_rows}")

    # compute the avarage of the numeric columns
    numeric_colums = columns_series.loc[columns_series.isin(['int64','float64'])]
    averages = {}
    for column_name in numeric_colums.keys():
        averages[column_name] = dataframe[column_name].mean()
    # print(f"computed averages={averages}")

    # return the categories in categorial columns
    categoral_colums = columns_series.loc[columns_series.isin(['category'])]
    categories = {}
    for column_name in categoral_colums.keys():
        t = list(dataframe[column_name].cat.categories)
        categories[column_name] = t if len(t) > 1 else "single category"
    # print(f"found categories={categories}")

    return {
        "column_names_correct": column_names_correct,
        "number_of_rows": number_of_rows,
        "averages": averages,
        "categories": categories
    }
