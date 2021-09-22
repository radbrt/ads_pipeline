import urllib3
from prefect import task, Flow, Parameter
from prefect.executors import DaskExecutor
from prefect.run_configs import LocalRun
import datetime
import coiled
import boto3
from google.oauth2 import service_account
import pandas as pd


def get_secret(secret_name):
    region_name = "eu-central-1"

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
        return get_secret_value_response['SecretString']


def save_file_to_s3(prefix, url):
    filename = str(datetime.date.today()) + ".json"
    bucket = boto3.client('s3')
    print("about to open load")
    http = urllib3.PoolManager()
    r = http.request('GET', url, preload_content=False).read()
    print("file read")
    bucket.put_object(Body=r, Bucket='radallelse', Key=f"enhetsregisteret/{prefix}/{filename}")
    print("written to bucket")
    return filename


def read_file_to_bq(prefix, filename):

    t = get_secret("gbq_accesskey")
    with open('bq_secret.json', 'w') as f:
        f.write(t)

    cred = service_account.Credentials.from_service_account_file("bq_secret.json")
    df = pd.read_json(f"s3://radallelse/enhetsregisteret/{prefix}/{filename}", compression='gzip', orient='record',
                      dtype={'organisasjonsnummer': 'str', 'overordnetEnhet': 'str'})
    df["updated_at"] = datetime.date.today()
    df.to_gbq(f"radjobads.radjobads.load_{prefix}", "radjobads", if_exists='replace', credentials=cred)


@task
def process_single_file(url):
    filename = save_file_to_s3(url['entity'], url['url'])
    read_file_to_bq(url['entity'], filename)

with Flow("hello-flow") as flow:

    urls = Parameter("urls", default=[
        {'entity': 'foretak',
         'url': "https://data.brreg.no/enhetsregisteret/api/enheter/lastned"},
        {'entity': 'virksomheter',
         'url': "https://data.brreg.no/enhetsregisteret/api/underenheter/lastned"}
    ])


    process_single_file.map(urls)


flow.run_config = LocalRun()

flow.executor = DaskExecutor(cluster_class=coiled.Cluster, cluster_kwargs={'software': 'radbrt/prefect_pipeline', 'n_workers': 2, 'worker_memory': "30 GiB"})

flow.register(project_name="er_pipe_load")
