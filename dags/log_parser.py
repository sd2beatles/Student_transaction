import re
import pandas as pd
import boto3
from decouple import config
from datetime import datetime
import awswrangler as wr
import time



    
    


def parse_log(logstring):
    try:
        expr=re.compile(r'.+/(?P<upload_date>\d{8})/(?P<file>.*.log):.*\[\[\]\]-(?:UserId:)(?P<user_id>.+)=(?:Action:)(?P<action>.*) ?(SUCCESS|ERROR) ?(?:ShortSession:)(?P<short>.*) ?(?:LongSession:)(?P<long>.*) ?\[loginApp\] ?VisangEduDuo ?(?:transaction:)\[(?P<trans>.*)\]-(?:URL:)(https://)?(?P<url>.*) (?:Path:)(?P<path>.*)')
        return expr.match(logstring)
    except:
        return None

def make_path(bucket,dt,folder=None):
    folder=folder if folder else 'student_info'
    path='s3://{}/{}/dt={}/student_file.parquet'.format(bucket,folder,dt)
    return path 


def parsed_lines(file_path,xcom_key,ti):
    #2 seconds break
    time.sleep(2)

    with open(file_path) as f:
        records=[]
        elements=['file','user_id','action','short','long','trans','url','path']
        for line in f:
            record=parse_log(line)
            if record:
                records.append({key:record.group(key)  for key in elements})
    
    ti.xcom_push(key=xcom_key,value=records)

    

        

            








