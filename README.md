# Federated RDB Summary

Algorithm based on the Vantage 6 Federated Summary for relational databases.
It reports the following information from each node:
- `Min`
- `Max`
- `Mean`
- `Pooled Standard Deviation`
- `Count`
- `Histogram`
- `Boxplot` (reported individually for each node)

Additionally, it's also possible to evaluate the number of participants for a cohort.

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
    image="pmateus/v6-summary-rdb:1.2.0",
    collaboration_id=1,
    input_= input_,
    organization_ids=[2]
)

# Retrieve the results
res = client.get_results(task_id=task.get("id")
```

### Histogram

The histogram function requires the bin width to be provided using the following variable `BIN_WIDTH`.

```python
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
                "functions": ["histogram"],
                "BIN_WIDTH": 4
            }
        ]
    }
}
```

### Boxplot

The Boxplot function allows to specify the IQR used to determine the boundaries.
By default this value is 1.5 but it can be changed using the following variable `IQR_THRESHOLD`.

```python
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
                "functions": ["boxplot"],
                "IQR_THRESHOLD": 2
            }
        ]
    }
}
```

### Cohort

The cohort function allows to explore possible groups of participants based on a set of
characteristics that can be set using the SQL operators:

```python
input_ = {
    "master": "true",
    "method":"master", 
    "args": [], 
    "kwargs": {
        "cohort": {
            "definition": [
                {
                    "variable": "Age",
                    "operator": ">=",
                    "value": 75
                },
                {
                    "variable": "deadstatus.event",
                    "operator": "=",
                    "value": 1
                },
                {
                    "variable": "Histology",
                    "operator": "IN",
                    "value": "('large_cell', 'scc')"
                }
            ],
            "table": "records",
            "id_column": "ID"
        }
    }
}
```

## Test / Develop

You need to have Docker installed.

To Build (assuming you are in the project-directory):
```
docker build -t v6-summary-rdb .
```
