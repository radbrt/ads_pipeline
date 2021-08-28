

import os
import time

import prefect
from prefect import task, Flow, Parameter
from prefect.executors import LocalExecutor
from prefect.run_configs import LocalRun
from datetime import datetime


@task
def say_hello(name):
    # Add a sleep to simulate some long-running task
    time.sleep(3)
    # Load the greeting to use from an environment variable
    greeting = os.environ.get("GREETING")
    logger = prefect.context.get("logger")
    logger.info(f"{greeting}, {name}!")
    logger.info(f"Running at {datetime.now()}")


with Flow("hello-flow") as flow:
    people = Parameter("people", default=["Arthur", "Ford", "Marvin"])
    say_hello.map(people)

# Configure the `GREETING` environment variable for this flow
flow.run_config = LocalRun(env={"GREETING": "Hello"})

# Use a `LocalDaskExecutor` to run this flow
# This will run tasks in a thread pool, allowing for parallel execution
flow.executor = LocalExecutor()


# Register the flow under the "tutorial" project
flow.register(project_name="ads_pipeline_local")
