from prefect import Flow
from prefect.run_configs import LocalRun
from prefect.executors import LocalExecutor
from prefect.tasks.dbt import DbtShellTask

with Flow(name="dbt_flow") as f:
    task = DbtShellTask(
        profiles_dir='/home/ec2-user/.dbt/'
    )(command='dbt run')


flow.run_config = LocalRun()

flow.executor = LocalExecutor()

flow.register(project_name="er_pipe_load", set_schedule_active=False)