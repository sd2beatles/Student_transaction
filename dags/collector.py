from cmath import log
from time import time
from airflow.utils.dates import days_ago
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow import DAG
from datetime import datetime,timedelta
from decouple import config
import log_parser
import dill
import os


dill.extend(False)

dag_path=os.getcwd()


default_args={
    'owner':'airflow',
    'depends_on_past':False,
    'start_date':datetime.utcnow(),
    'email':['airflow@airflow.com'],
    'email_on_failure':False,
    'email_on_retry':False,
    'retries':1,
    'retry_delay':timedelta(minutes=5),
    'catchup':False
}

dag=DAG('data_collection',default_args=default_args,schedule_interval=timedelta(1))


# bash_command="sh {{params.rawdata_folder}}/merge_log.sh {{params.rawdata_folder}}"

# rawdata_folder=os.path.join(dag_path,'raw_data','transaction')

def test(file_path,s3_bucket,bucket_folder,ti):
    data=ti.xcom_pull(key='df',task_ids=['s3_check'])
    print(data)
    print(file_path)
    print("yes")

def test2(ti):
    value='david'
    ti.xcom_push(key='david',value=value)

def test3():
    print("s")

s3_connection=PythonOperator(
    task_id='s3_check',
    python_callable=test2,
    dag=dag) 





success=PythonOperator(
        task_id='final',
        python_callable=test3,
        dag=dag
    )

tasks=['transaction','login']

for task in tasks:
    rawdata_folder=os.path.join(dag_path,'raw_data',task)
    bash_command="sh {{params.rawdata_folder}}/merge_log.sh {{params.rawdata_folder}} {{params.file_name}}"

    merged=BashOperator(
        task_id=f'log_merge_{task}',
        bash_command=bash_command,
        params={'rawdata_folder':rawdata_folder,'file_name':task},
        dag=dag)

    file_path=os.path.join(rawdata_folder,'result.text')
    save_to_s3=PythonOperator(
        task_id=f'parse_log_{task}',
        python_callable=test,
        op_kwargs={
            'file_path':file_path,
            's3_bucket':config("S3_BUCKET"),
            'bucket_folder':'stduent_info'
        },
        dag=dag
    )
    s3_connection >> merged >> save_to_s3 >> success








