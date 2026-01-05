from airflow import DAG
from airflow.operators.bash import BashOperator
import datetime

OWNER = 'orocko'
DAG_ID = 'dbt'

args = {
    'owner': OWNER,
    'start_date': datetime.datetime(2025,12,1),
    'catchup': False,
}

dag = DAG(dag_id=DAG_ID, default_args=args, schedule='@daily')

dbt_run = BashOperator(
    task_id='dbt_run',
    bash_command='cd /opt/dbt_click; dbt build',
    dag=dag
)