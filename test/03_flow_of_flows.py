import os
import time

import prefect
from prefect import task, Flow, Parameter
from prefect.executors import LocalExecutor
from prefect.run_configs import LocalRun
from datetime import datetime
from prefect.tasks.prefect import StartFlowRun


flow1 = StartFlowRun(flow_name="hello-flow", project_name="jottings")

flow2 = StartFlowRun(flow_name="world-flow", project_name="jottings")



with Flow("flowyflow") as ff:
    logger = prefect.context.get("logger")
    logger.info("flowyflow running")
    c = flow2(upstream_tasks=[flow1])



ff.run_config = LocalRun()

ff.executor = LocalExecutor()

ff.register(project_name="jottings")
