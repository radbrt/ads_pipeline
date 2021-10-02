from prefect import Flow
from prefect.run_configs import LocalRun
from prefect.executors import LocalExecutor
from prefect.tasks.dbt import DbtShellTask

with Flow(name="dbt_flow") as f:
    task = DbtShellTask(
        profiles_dir='/home/ec2-user/.dbt/',
        helper_script='cd /home/ec2-user/prefect_pipeline/etl/prefect_er'
    )(command='dbt run')


f.run_config = LocalRun()

f.executor = LocalExecutor()

f.register(project_name="er_pipe_load", set_schedule_active=False)