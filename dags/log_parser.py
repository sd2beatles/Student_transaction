import os 
import re
import pandas as pd
import boto3
from decouple import config
from datetime import datetime
import awswrangler as wr


def connect():
    access_key,secret_key=config("AWS_ACCESS_KEY",cast=str),config("AWS_SECRET_KEY",cast=str)
    default_region=config("DEFAULT_REGION")

        #Add if you want to make use of s3 resource 
        # s3=boto3.client("s3",
        #             aws_access_key_id=access_key,
        #             aws_secret_access_key=secret_key,
        #             region_name=default_region)

    session=boto3.Session(aws_access_key_id=access_key,
                            aws_secret_access_key=secret_key,
                            region_name=default_region)
    
   
    


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


def save_to_s3(file_path,s3_bucket,bucket_folder='student_info'):

    with open(file_path) as f:
        records=[]
        elements=['file','user_id','action','short','long','trans','url','path']
        for line in f:
            record=parse_log(line)
            try:
                if record:
                    records.append({key:record.group(key)  for key in elements})
            except:
                pass
    session=connect()
    dt=datetime.datetime.utcnow().strftime('%Y-%m-%d')
    path=make_path(s3_bucket,dt,bucket_folder)
    #make dataframe 
    student_info=pd.DataFrame(records)
    wr.s3.to_parquet(pd.DataFrame(records),path=path)
    print("Data has successfully been fetched to {}".format(path))

        
    
        

            








