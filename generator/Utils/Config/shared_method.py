import concurrent.futures
import re
import pandas as pd 
from Errors.errors import LangaugeSupportError,InputDataError,DateTypeError
import datetime
import boto3
import json

def multiprocessing_task(data,parser,cores,data_size,fn):
    interval=data_size//cores
    remain=data_size-cores*interval
    merged_results=list()
    with concurrent.futures.ProcessPoolExecutor() as executor:
        outcomes=[executor.submit(fn,data,parser,i,i*interval,(i+1)*interval) for i in range(cores)]
        for f in concurrent.futures.as_completed(outcomes):
            merged_results+=f.result()

            
    return merged_results,remain



def check_input_path(input_path,date):
    assert type(date)==str,'date must be string'
    path_search=re.search('[\w\-\_]+\.{1}(json|parquet|csv)$',input_path)
    if not path_search:
        raise InputDataError

    date_search=re.search('([0-9]{4}-[0-9]{1,2}-[0-9]{1,2})',date)
    if not date_search:
        raise DateTypeError

    return path_search.group(1),date_search.group(1)





def load_data(input_path,s3_bucket_name,file_name,date='now',folder=None,convert_to_parquet=False):
    if date=='now':
        date=datetime.datetime.utcnow().strftime('%Y-%m-%d')
    format,dt=check_input_path(input_path,date)
    
    if convert_to_parquet:
        if format!='csv':
            data_json=open(input_path,'rb')
            df=pd.read_json(data_json)
        else:
            df=pd.read_csv(input_path)
        df.to_parquet('./{}.parquet'.format(file_name))
        input_path='./{}.parquet'.format(file_name)
    
    data=open(input_path,'rb')
    s3=boto3.resource('s3')
    if folder:
        path='{}/dt={}/{}.{}'.format(folder,dt,file_name,format)
    else:
        path='dt={}/{}.{}'.format(dt,file_name,format)
    object=s3.Object(s3_bucket_name,path)
    object.put(Body=data)
    print('Data has been sucessfully loaded to the assigned folder')


    
class MetaConst(type):
    def __getattr__(cls,key):
        return cls[key]
    def __setattr__(cls,key,value):
        return ValueError


class Const(object,metaclass=MetaConst):
    def __getattr_(self,name):
        return self[name]
    def __setattr__(self, name, value):
        raise TypeError
    
