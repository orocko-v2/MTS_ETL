import Requests

import logging
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import datetime
import json
import time
from io import BytesIO
import pandas as pd
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


OWNER = 'orocko'
DAG_ID = 'extract_data'
MINIO_BUCKET_NAME = "phones"

args = {
    'owner': OWNER,
    'start_date': datetime.datetime(2025,12,1),
    'catchup': False,
}

def summarize(rqst):
    sum = 0
    for oper in rqst.json()['Usages']:
        if oper['type'] != 'income':
            sum += oper['amount']
    return sum

dag = DAG(
    dag_id=DAG_ID,
    default_args=args,
    schedule='@daily'
)

def get_data_from_api(**kwargs):
    logging.info('start')

    folder_name=str(datetime.date.today())

    csv_path = "./dags/data/phones.csv"
    df = pd.read_csv(csv_path, encoding='windows-1252', sep=';')
    phones = df.iloc[:, 0].tolist()

    hook = S3Hook(aws_conn_id='s3_conn')
    

    logging.info(f"Start loading for date {datetime.date.today()}")
    
    Requests.token = Requests.get_access_token()

    for phone in phones:
        i=0
        successful = False
        params_list=[phone, \
                     datetime.datetime.now() - datetime.timedelta(days=1),  \
                     datetime.datetime.now()]
        while not successful:
            try:
                responce = Requests.create_request(flag='BILLS_BY_MSISDN', params_list = params_list)
                if responce.status_code == requests.codes.ok:
                    successful = True
                    output = responce.json()
                    filename = f"{folder_name}/{str(phone)}_{datetime.datetime.now()}.json"
                else:
                    time.sleep(10)
            except requests.exceptions.HTTPError as e:
                if (int(e.args[0][0:3]) == 429):
                    time.sleep(10)
                    continue
                elif (int(e.args[0][0:3]) == 401) and i < 2:
                    Requests.token = Requests.get_access_token()
                    time.sleep(60)
                    i+=1
                    continue
                else:
                    successful = True
                    continue
            
            hook.load_string(
                string_data=json.dumps(output),
                key=filename,
                bucket_name=MINIO_BUCKET_NAME,
                replace=True
            )

            logging.info(f"JSON object {filename} was successfully uploaded to bucket {MINIO_BUCKET_NAME}")

def create_bucket(**kwargs):
    hook = S3Hook(aws_conn_id='s3_conn')
    if not hook.check_for_bucket(MINIO_BUCKET_NAME):
        hook.create_bucket(MINIO_BUCKET_NAME)
        logging.info(f'{MINIO_BUCKET_NAME} was created')
    else:
        logging.info(f'{MINIO_BUCKET_NAME} already exists')


create_bucket_task = PythonOperator(
    task_id='create_bucket',
    python_callable=create_bucket,
    dag=dag
)

get_data_from_api_task = PythonOperator(
    task_id='get_data_from_api',
    python_callable=get_data_from_api,
    dag=dag
) 

trigger_clickhouse_load = TriggerDagRunOperator(
    task_id='trigger_clickhouse_load',
    trigger_dag_id='load_data_to_ch',
)
create_bucket_task >> get_data_from_api_task >> trigger_clickhouse_load