# Federated RDB Summary

Algorithm based on the Vantage 6 Federated Summary for relational databases. It reports the `Min`, `Mean`, `Max` from each `Node`.

## Possible Privacy Issues

🚨 Categorial column with only one category <br />
🚨 `Min` an `Max` for each column is reported <br />
🚨 Column names can be guessed, by trail and error

## Privacy Protection

✔️ If column names do not match nothing else is reported <br />
✔️ If dataset has less that 10 rows, no statistical analysis is performed <br />
✔️ Only statistical results `Min`, `Mean`, `Max` are reported.

## Usage
```python
from vantage6.client import Client
from pathlib import Path

# Create, athenticate and setup client
client = Client("http://127.0.0.1", 5000, "")
client.authenticate("researcher@center.nl", "password")
client.setup_encryption(None)

# Define algorithm input
# The summary functions to be computed for each column will be selected in the following order:
# 1. the functions provided for a specific column
# 2. the functions provided for all columns
# 3. all functions will be computed
input_ = {
    "master": "true",
    "method":"master", 
    "args": [], 
    "kwargs": {
        #"functions": ["min", "max"],
        "columns": [
            {
                "variable": "age",
                "table": "records",
                #"functions": ["min", "max"]
            },
            {
                "variable": "Clinical.T.Stage",
                "table": "records"
            }
        ]
    }
}

# Send the task to the central server
task = client.post_task(
    name="summary",
    image="pcmateus/v6-summary-rdb",
    collaboration_id=1,
    input_= input_,
    organization_ids=[2]
)

# Retrieve the results
res = client.get_results(task_id=task.get("id")
```

## Test / Develop

You need to have Docker installed.

To Build (assuming you are in the project-directory):
```
docker build -t v6-summary-rdb .
```

To test/run locally the folder `local` is included in the repository. The following command mounts these files and sets the docker `ENVIROMENT_VARIABLE` `DATABASE_URI`.
```
docker run -e DATABASE_URI=/app/database.csv -v .\local\input.txt:/app/input.txt -v .\local\output.txt:/app/output.txt -v .\local\database.csv:/app/database.csv harbor.vantage6.ai/algorithms/summary
```
