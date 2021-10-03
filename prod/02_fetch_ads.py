import urllib3
from prefect import task, Flow, unmapped
from prefect.executors import DaskExecutor
from prefect.executors import LocalExecutor
from prefect.run_configs import LocalRun
import prefect
import coiled
from coiled import Cluster
import boto3
import time
import json
import requests
from datetime import datetime, timedelta
import base64
from botocore.exceptions import ClientError
import dask.dataframe as dd
import pandas as pd
from dask.distributed import Client
from google.oauth2 import service_account
from pathlib import Path
import pytz

logger = prefect.context.get("logger")

def get_secret(secret_name, region_name="eu-central-1"):
    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        raise e
    else:
        if 'SecretString' in get_secret_value_response:
            secret = get_secret_value_response['SecretString']
        else:
            secret = base64.b64decode(get_secret_value_response['SecretBinary'])

    return secret


def check_key(fileloc):
    keyfile = Path(fileloc)
    if not keyfile.is_file():
        t = get_secret("gbq_accesskey")
        with open(fileloc, 'w') as f:
            f.write(t)

@task()
def save_ad_page(ad_list):
    # cluster = coiled.Cluster(n_workers=1, name="prefect_ads_processing",
    #                          software="radbrt/prefect_pipeline",
    #                          worker_memory="14 GiB")
    # client = Client(cluster)
    # print("Dashboard:", client.dashboard_link)

    logger.info(f"Saving: {len(ad_list['content'])} pages")
    check_key("bq_secret.json")

    df = pd.DataFrame(ad_list['content'])
    cred = service_account.Credentials.from_service_account_file("bq_secret.json")
    df.to_gbq("radjobads.radjobads.wrk_job_ads", "radjobads", if_exists='append', credentials=cred)


@task(max_retries=3, retry_delay=timedelta(minutes=5))
def fetch_single_page(page, endpoint, header, args):
    time.sleep(1)
    request_string = f"{endpoint}?{args}&page={page}"
    webrequest = requests.get(request_string, headers=header)
    logger.info(f"Fetched page number {page}")
    assert webrequest.status_code == 200

    ads_page = json.loads(webrequest.text)
    save_ad_page.run(ads_page)

    return request_string


@task(max_retries=3, retry_delay=timedelta(minutes=5))
def start_fetching(start_isotime, end_isotime):
    ENDPOINT = 'https://arbeidsplassen.nav.no/public-feed/api/v1/ads'
    TOKEN = json.loads(get_secret("ads_api_token"))
    HEADERS = {"accept": "application/json", "Authorization": f"Bearer {TOKEN['api_token']}"}

    endtime = end_isotime
    args = f"size=100&published=%5B{start_isotime}%2C{endtime}%5D"

    r = requests.get(f"{ENDPOINT}?{args}", headers=HEADERS)

    assert r.status_code == 200

    ads = json.loads(r.text)
    total_pages = ads['totalPages']
    logger.info(f"Total pages: {total_pages}")
    logger.info(f"total elements")

    saveresult = save_ad_page.run(ads)
    logger.info(f"Saveresult: {saveresult}")

    if total_pages > 1:
        for page in range(1, total_pages + 1):
            fetch_single_page.run(page, endpoint=ENDPOINT, header=HEADERS, args=args)


@task()
def register_time(runtime):
    runtime_iso = runtime.isoformat()
    prefect.backend.kv_store.set_key_value('last_ads_run', {'last_run': runtime_iso})


with Flow("fetch_ads") as flow:
    logger.info(f"init")
    last_run = prefect.backend.kv_store.get_key_value('last_ads_run')['last_run']
    last_run_ts = datetime.fromisoformat(last_run)
    start_isotime = last_run_ts.replace(tzinfo=None).isoformat(timespec='seconds')

    current_time = datetime.now(pytz.timezone('Europe/Oslo'))
    logger.info(f"starting at timestamp {start_isotime}")
    current_time_string = current_time.replace(tzinfo=None).isoformat(timespec='seconds')
    logger.info(f"Ending at timestamp {current_time_string}")

    start_fetching(start_isotime, current_time_string)

    register_time(current_time)


flow.run_config = LocalRun()

flow.executor = LocalExecutor()

# flow.executor = DaskExecutor(cluster_class=coiled.Cluster,
#                              cluster_kwargs={'software': 'radbrt/prefect_pipeline', 'n_workers': 1,
#                                              'worker_memory': "14 GiB"})

flow.register(project_name="er_pipe_load", set_schedule_active=False)

# flow.run()
