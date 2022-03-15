from multiprocessing.sharedctypes import Value
from tabnanny import check
from zlib import DEF_BUF_SIZE
from decouple import config as dc_config
from Errors.errors import LangaugeSupportError
from Utils.Const.info_const import *
from Utils.Config.parser import argparse_student_info
from Utils.Config.shared_method import *
from faker import Faker
from scipy import stats
import numpy as np
import multiprocessing
import concurrent.futures
import json
import boto3
import re
import pandas as pd
import datetime
import pymysql



def create_date(year,month,day_start,hours_start,minutes_start,day_end,hours_end,minutes_end):
    if month==2:
        day_end=min(day_end,28)
    days=[day_start,day_end]
    hours=[hours_start,hours_end]
    minutes=[minutes_start,minutes_end]
    date_='{}-{}-{}:{}:{}'.format(year,str(month).zfill(2),str(np.random.randint(days[0],days[1])).zfill(2),\
        str(np.random.randint(hours[0],hours[1])).zfill(2),str(np.random.randint(minutes[0],minutes[1]))).zfill(2)
    return date_


def make_study_hours(grade):
    record=params[grade]
    a,b=record['alpha'],record['beta']
    x=np.random.uniform()
    weekly_study_hours=np.round(float(stats.beta.ppf(x,a,b)*100),2)
    daily_stduy_hours=np.round(float(weekly_study_hours/7+x),2)
    is_online=np.random.choice([0,1],p=is_online_ratio)
    off_line=np.random.choice(record['offline_course'])
    satisfaction_last=np.random.choice(record['satisfaction_last'],p=satisfaction_ratio)
    no_interest=np.random.choice(record['no_interest'],p=no_interest_ratio)
    no_prizes=np.random.poisson(lam=record['possion'][0])+np.random.choice(record['possion'][1])
    no_competions=np.ceil(record['competition_factor']*no_prizes)
    if no_competions<no_prizes:
        no_competions+=2
    return {'grade':str(grade),'weekly_study_hours':float(weekly_study_hours),'daily_study_hours':float(daily_stduy_hours),'is_online':int(is_online),
             'off_line_numbers':int(off_line),'satisfaction_last':int(satisfaction_last),'no_interest':int(no_interest),'no_prizes':int(no_prizes),
             'no_competitions':int(no_competions)}


def make_record(year,month,day_start,hours_start,minutes_start,day_end,hours_end,minutes_end,language='en'):
    if language=='en':
        faker=Faker()
    elif language=='kor':
        faker=Faker('ko_KR')
    else:
        raise LangaugeSupportError
        
    register_date=create_date(year,month,day_start,hours_start,minutes_start,day_end,hours_end,minutes_end)
    grade=np.random.choice(grades,p=ratio)
    record=make_personal_record(faker)
    record.update(make_study_hours(grade))
    record.update({'age':np.random.randint(14,19),'register_date':register_date})
    return record 


def make_personal_record(faker):
    record=faker.profile()
    name,sex,user_id=record['name'],record['sex'],record['username']
    residence,current_location=record['residence'],str(record['current_location'][0])+'|'+str(record['current_location'][1])
    mail=record['mail']
    return {'name':name,'sex':sex,'user_id':user_id,'address':residence,'location':current_location,'mail':mail}
    





def crate_group_record(data,config,core_index,start,finish):
    
    inner_results=[]
    print('{} core starts'.format(core_index))
    for _ in range(start,finish):
        record=make_record(config.year,config.month,config.day_start,config.hours_start,config.minutes_start,config.day_end,config.hours_end,config.minutes_end,
                      config.language)
        inner_results.append(record)
    print('{} core ends'.format(core_index))
    return inner_results



def singleTask(config,length):
    student_info=list()
    for _ in range(length):
        record=make_record(config.year,config.month,config.day_start,config.hours_start,config.minutes_start,config.day_end,config.hours_end,config.minutes_end,
                   config.language)
        student_info.append(record)
    return student_info


'''
Rrequirements for the transfer of data to s3 bucket

1)Following are the allowed data types for transfer to s3:
   - csv
   - json
   - parquet

2) All data are stored and managed as the hive-compatible partitions.

3) Date must follow the format like '%Y-%m-%d' and its type must be string. 
'''

class DBConnectionError(ValueError):
    def __init__(self,msg="Connection to the assigned DB has failed."):
        super().__init__(msg)


import sys
def check_db_info(db_info):
    assert type(db_info)==dict
    try:
        conn=pymysql.connect(
            host=dc_config(db_info['host']),
            port=int(dc_config(db_info['port'])),
            user=dc_config(db_info["user"]),
            database=dc_config(db_info["database"]),
            password=dc_config(db_info["password"]),
            use_unicode=True,
            charset='utf8')
        cursor=conn.cursor()
        return conn,cursor
    except:
        raise DBConnectionError
    
def insert_row(cursor,data,table):
    assert type(data)==dict
    
    placeholder=','.join(['%s']*len(data))
    fields=','.join(data.keys())
    key_placeholder=','.join(['{}=%s'.format(col) for col in data.keys()])
    sql='INSERT INTO %s (%s) VALUES (%s) ON DUPLICATE KEY UPDATE %s' %(table,fields,placeholder,key_placeholder)
    
    cursor.execute(sql,list(data.values())*2)





def main(config):

    if config.multiprocessing_on:
        data_size=config.data_length
        cores=min(multiprocessing.cpu_count(),config.core_count)
        data=None
        student_info,remain=multiprocessing_task(data,config,cores,data_size,crate_group_record)
        print("s")
        if remain>0:
            student_info+=singleTask(config,remain)
    else:
        student_info=singleTask(config,config.data_length)
    
    if config.db_info:
        db_info={
        'host':'student_info_host',
        'port':'student_info_port',
        'user':'student_info_user',
        'database':'student_info_database',
        'password':'student_info_password'}

        conn,cursor=check_db_info(db_info)
        table="Student_info"
        for record in student_info:
            insert_row(cursor,record,table)
        conn.commit()
        conn.close()
        print("All the records are successfully transfered to the assigned table")

   
    if config.output_path:
        with open(config.output_path,'w') as f:
            if config.language=='kor':
                json.dump(student_info,f,ensure_ascii=False,indent=2)
            else:
                json.dump(student_info,f,indent=2)

    
    if config.s3_full_path:
        path=re.search('s3://([\w\W]+)',config.s3_full_path).group(1)
        s3_bucket_name,folder=path.split('/'),'/'.join([ i for i in path.split('/')[1:] if i])
        load_data(input_path=config.output_path,s3_bucket_name=s3_bucket_name,parquet_name='student-info',
               folder=folder,date=config.s3_upload_date)
if __name__ == '__main__':
    config=argparse_student_info()
    main(config)
    

    
# eg)python generate_student_info.py --language kor --year 2020 --month 2 --day_end 14 --data_length 10 --multiprocessing_on 1 --core_count 2 --output_path ./input_storage/student_info_temp.json
    

    

#python generate_student_info.py --language kor --year 2022 --month 3 --day_end 30 --data_length 1000 --output_path ./input_storage/2022-03-01/student_info_temp.json