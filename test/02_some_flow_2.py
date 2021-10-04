import os
import time

import prefect
from prefect import task, Flow, Parameter
from prefect.executors import LocalExecutor
from prefect.run_configs import LocalRun
from datetime import datetime



@task
def say_world(name):
    logger.info(f"{name}'s world")
    logger.info(f"Running at {datetime.now()}")



with Flow("world-flow") as wf:
    logger = prefect.context.get("logger")
    logger.info("Worldflow starting")
    say_world.map(["gunderius", "sigmalina"])



wf.run_config = LocalRun()

wf.executor = LocalExecutor()

wf.register(project_name="jottings")
