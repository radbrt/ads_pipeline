import urllib3
from prefect import task, Flow, Parameter
from prefect.executors import DaskExecutor
from prefect.run_configs import LocalRun
import prefect
import coiled
import boto3
# import pymongo
import time
import json
import requests
from datetime import datetime, timedelta
import base64
from botocore.exceptions import ClientError
import pandas as pd
from google.oauth2 import service_account


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


def insert_ads(adsarray, db_table):
    errors = []
    insert_attempts = 0
    for ad in adsarray:
        try:
            time.sleep(0.01)
            db_table.insert_one(ad)
            insert_attempts += 1
        except Exception as e:
            errors.append({"ad": ad, "error": e})
    return insert_attempts, errors




@task
def save_ad_page(ad_list):
    df = pd.DataFrame(ad_list.get('content'))
    cred = service_account.Credentials.from_service_account_file("bq_secret.json")
    df.to_gbq("radjobads.radjobads.wrk_job_ads", "radjobads", if_exists='append', credentials=cred)


@task
def fetch_single_page(page, endpoint, header, args):
    time.sleep(1)
    request_string = f"{endpoint}?{args}&page={page}"

    # r = requests.get(request_string, headers=header)

    # if r.status_code == 200:
    #     ads = json.loads(r.text)
    #     attempts, errors = insert_ads(ads['content'], jobs_db.ads)
    #     print(f"Page {current_page}, Attempts: {attempts}, Errors: {len(errors)}")
    # else:
    #     print('Non-200 return code')


    print(request_string)
    ad_list = {
        "content": [
            {
                "uuid": "e3c9d2df-fdc6-41f4-bd2a-e235e75e493f",
                "published": "2021-09-24T23:00:00Z",
                "expires": "2021-10-08T22:00:00Z",
                "updated": "2021-09-24T23:00:00.032296Z",
                "workLocations": [{"municipal": "BERGEN"}],
                "title": "Vi søker en flink vernepleier",
                "description": " schnell",
                "sourceurl": null,
                "source": "XML_STILLING",
                "applicationUrl": null,
                "applicationDue": "2021-10-09T00:00",
                "occupationCategories": [
                    {
                        "level1": "Helse og sosial",
                        "level2": "Helse"
                    }
                ],
                "jobtitle": null,
                "link": "https://arbeidsplassen.nav.no/stillinger/stilling/e3c9d2df-fdc6-41f4-bd2a-e235e75e493f",
                "employer": {
                    "name": "Dagtjenester for utviklingshemmede, Bergen kommune",
                },
                "engagementtype": "Fast",
                "extent": "Heltid",
                "starttime": null,
                "positioncount": "1",
                "sector": "Offentlig"
            }
        ]
    }

    if 1 == 1: #r.status_code == 200:
        #ads = json.loads(r.text)
        save_ad_page(ad_list).run()
    else:
        print('Non-200 return code')

    return request_string


with Flow("fetch_ads") as flow:
    ENDPOINT = 'https://arbeidsplassen.nav.no/public-feed/api/v1/ads'
    TOKEN = json.loads(get_secret("ads_api_token"))
    HEADERS = {"accept": "application/json", "Authorization": f"Bearer {TOKEN['api_token']}"}

    t = get_secret("gbq_accesskey")
    with open('bq_secret.json', 'w') as f:
        f.write(t)



    #last_run = datetime.today() - timedelta(hours=1, minutes=15)
    #start_isotime = last_run.isoformat(timespec='seconds')
    last_run = prefect.backend.kv_store.get_key_value('last_ads_run')['last_run']
    last_run_ts = datetime.fromisoformat(last_run_ts)
    start_isotime = last_run_ts.isoformat(timespec='seconds')
    endtime = "*"

    args = f"size=100&published=%5B{start_isotime}%2C{endtime}%5D"
    # secret = json.loads(get_secret("mongodb"))
    # client = pymongo.MongoClient(
    #     f"mongodb+srv://radbrt:{secret['radbrt']}@cluster0-eoh9n.mongodb.net/jobs?retryWrites=true&w=majority")
    # jobs_db = client.jobs

    current_page = 0

    r = requests.get(f"{ENDPOINT}?{args}&page={current_page}", headers=HEADERS)
    ads = json.loads(r.text)

    # attempts, errors = insert_ads(ads['content'], jobs_db.ads)
    # print(f"Page {current_page}, Attempts: {attempts}, Errors: {len(errors)}")
    max_page = ads.get('totalPages') + 1 or 0

    if max_page > 1:
        pages = range(1, max_page)
        fetch_single_page.map(pages, task_args={'endpoint': ENDPOINT, 'header': HEADERS, 'args': args})

    # while current_page <= max_page:
    #     time.sleep(5)
    #     current_page += 1
    #     r = requests.get(f"{ENDPOINT}?{args}&page={current_page}", headers=HEADERS)
    #     if r.status_code == 200:
    #         ads = json.loads(r.text)
    #         attempts, errors = insert_ads(ads['content'], jobs_db.ads)
    #         print(f"Page {current_page}, Attempts: {attempts}, Errors: {len(errors)}")
    #     else:
    #         print('Non-200 return code')

flow.run_config = LocalRun()
flow.executor = DaskExecutor(cluster_class=coiled.Cluster,
                             cluster_kwargs={'software': 'radbrt/prefect_pipeline', 'n_workers': 2,
                                             'worker_memory': "14 GiB"})
flow.register(project_name="er_pipe_load")
