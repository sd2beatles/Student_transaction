from cmath import log
from time import time
from airflow.utils.dates import days_ago
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow import DAG
from datetime import datetime,timedelta
from decouple import config
from log_parser import parsed_lines
import awswrangler as wr
import pandas as pd
import dill
import boto3
import pandas
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

def connect():
    access_key,secret_key=config("AWS_ACCESS_KEY",cast=str),config("AWS_SECRET_KEY",cast=str)
    default_region=config("DEFAULT_REGION")

    session=boto3.Session(aws_access_key_id=access_key,
                            aws_secret_access_key=secret_key,
                            region_name=default_region)
    return session
    


start=BashOperator(
    task_id='start_task',
    bash_command='echo Data pipeline is about to start',
    dag=dag)

    
def make_path(bucket,dt,folder=None):
    folder=folder if folder else 'student_info'
    path='s3://{}/{}/dt={}/student_file.parquet'.format(bucket,folder,dt)
    return path 

def save_to_s3(task,s3_bucket,bucket_folder,ti):
    records=ti.xcom_pull(key=task,task_ids=[f'parse_log_{task}'])
    session=connect()
    dt=datetime.datetime.utcnow().strftime('%Y-%m-%d')
    path=make_path(s3_bucket,dt,bucket_folder)
    
    #make dataframe 
    records=pd.DataFrame(records)
    wr.s3.to_parquet(pd.DataFrame(records),path=path,boto3_session=session)
    print("Data has successfully been fetched to {}".format(path))



success=BashOperator(
    task_id='final_task',
    bash_command='echo Datapipe line has been sucessfully established',
    dag=dag)

tasks=['transaction','login']

for task in tasks:
    rawdata_folder=os.path.join(dag_path,'raw_data',task)
    bash_command="sh {{params.rawdata_folder}}/merge_log.sh {{params.rawdata_folder}} {{params.file_name}}"

    merged=BashOperator(
        task_id=f'log_merge_{task}',
        bash_command=bash_command,
        params={'rawdata_folder':rawdata_folder,'file_name':task},
        dag=dag)
    
    #def parse_log(file_path,xcom_key):
    file_path=os.path.join(rawdata_folder,f'{task}.txt')
    parsed=PythonOperator(
        task_id=f'parse_log_{task}',
        python_callable=parsed_lines,
        op_kwargs={
            'file_path':file_path,
            'xcom_key':task},
        dag=dag)
   
    store_data=PythonOperator(
        task_id=f'store_{task}',
        python_callable=save_to_s3,
        op_kwargs={
            'task':task,
            's3_bucket':config("S3_BUCKET"),
            's3_folder':task},
        dag=dag)

    start>> merged >> parsed >>store_data>> success








