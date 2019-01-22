|:warning: Privacy Preserving Distributed Learning (ppdDLI) |
|------------------|
| This algorithm is part of the [ppDLI](https://github.com/IKNL/ppDLI). A docker build of this algorithm can be obtained from docker-registry.distributedlearning.ai/dsummary |

# Distributed Summary
Algorithm that is inspired by the `Summary` function in R. It report the `Min`, `Q1`, `Mean`, `Median`, `Q3`, `Max` and number of `Nan` values per column from each `Node`. 

## Possible Privacy Issues

🚨 Categorial column with only one category <br />
🚨 `Min` an `Max` for each column is reported <br />
🚨 Column names can be geussed, by trail and error

## Privacy Protection

✔️ If column names do not match nothing else is reported <br />
✔️ If dataset has less that 10 rows, no statistical analysis is performed <br />
✔️ Only statistical results `Min`, `Q1`, `Mean`, `Median`, `Q3`, `Max` and number of `Nan` values per column are reported.

## Input.txt
```
{
  "method":"summarize", 
  decimal: ",", 
  "sep":";", 
  "columns":{
    "patient_id": "Int64",
    "age": "Int64",
    "weight": "float64",
    "stage": "category", 
    "cat": "category", 
    "hot_encoded":"Int64"
   }
}
```

## Test / Develop

You need to have Docker installed.

To Build (assuming you are in the project-directory):
```
docker build -t dsummary .
```

To test/run locally the folder `local` is included in the repository. The following command mounts these files and sets the docker `ENVIROMENT_VARIABLE` `DATABASE_URI`.
```
docker run -e DATABASE_URI=/app/database.csv -v .\local\input.txt:/app/input.txt -v .\local\output.txt:/app/output.txt -v .\local\database.csv:/app/database.csv dsummary
```
