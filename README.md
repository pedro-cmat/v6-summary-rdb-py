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

## Node Setup

Make sure to set the database connection parameters as environment variables using the default variables for a postgres database (https://www.postgresql.org/docs/9.3/libpq-envars.html):

```yaml
    application:
        ...
        algorithm_env:
            PGUSER: <user>
            PGPASSWORD: <password>
            PGDATABASE: <database>
            PGPORT: <port>
            PGHOST: <host>
```

## Usage
```python
from vantage6.client import Client

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
        "functions": [],
        "columns": [
            {
                "variable": "age",
                "table": "records",
                "functions": ["min", "max"]
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
    image="pmateus/v6-summary-rdb:1.0.0",
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
