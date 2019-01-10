


def summarize_dataframe(dataframe, column_names_valid: list=[]):
    column_names = list(dataframe.columns)
    
    valid_column_names = column_names == column_names_valid
    
    number_of_rows = dataframe["patient_id"].size
    
    average_weight = dataframe["weight"].mean()
    
    average_age = dataframe["age"].mean()
    
    stages = list(dataframe["stage"].astype('category'))
    
    return {
        "valid_column_name": valid_column_names,
        "number_of_rows": number_of_rows,
        "average_weight": average_weight,
        "average_age": average_age,
        "stages": stages
    }


    