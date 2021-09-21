"""Researcher (websockets but no central container)

Instead of polling the servers a User/Reseacher can also connect to the
websocket interface of the server. This allows the server to notify the
researcher on any status updates.

Reference:
https://distributedlearning.readme.io/docs/node-server-communication
"""

from socketIO_client import SocketIO, SocketIONamespace
from vantage6.client import Client


class TasksNamespace(SocketIONamespace):

    def on_status_update(self, result_id):

        print("socket call!")

        results = client.get_results(task_id=task.get("id"))

        # print the results per node
        for result in results:
            node_id = result.get("node")
            print("-"*80)
            print(f"Results from node = {node_id}")
            print(result.get("result"))


# central server configuration
host = "http://192.168.37.1"
port = 5000
collaboration_id = 3

# 1. authenticate to the central server
client = Client(
    host=host,
    port=port,
    path="/api"
)
client.setup_encryption(None)
client.authenticate("root", "admin")

# 2. connect to websocket interface
bearer_token = client.token
socket = SocketIO(host, port=port, headers={
    "Authorization": f"Bearer {bearer_token}"
})

# subscribe to the websocket channel.
taskNamespace = socket.define(TasksNamespace, "/tasks")
taskNamespace.emit("join_room", f"collaboration_{collaboration_id}")

# input for the dsummary Docker image (algorithm)
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
                "functions": ["min", "max"]
            },
            {
                "variable": "weight",
                "table": "records"
            }
        ]
    }
}

# post the task to the server
task = client.post_task(
    name="summary",
    image="pcmateus/v6-summary-rdb:1.2.0",
    collaboration_id=collaboration_id,
    input_=input_
)

socket.wait(seconds=100)
