from cmath import inf
from hashlib import new
from multiprocessing.sharedctypes import Value
from sqlite3 import paramstyle
from tkinter import E
from xmlrpc.client import Boolean
import numpy as np
import re
from Errors.errors import SubjectCodeError,AgeCodeError,InputDataError
import argparse
import datetime
from Utils.Const.trans_const import *
from Utils.Config.shared_method import Const
from decouple import config as dc_config
import sys
from Utils.Config.parser import argparse_transaction
import pandas as pd
import uuid
import pymysql
import json
import uuid
import os


class const(Const):
    main_category=main_category
    terms=terms
    urls=urls
    service_code_tuple=(main_category['info']['service_codes'],main_category['info']['service_pr'])


val=const()
main_category=val.main_category
terms=val.terms
urls=val.urls 
service_code_tuple=val.service_code_tuple





def date_conversion(format,time_format=None,string_format=None):
    if string_format:
        return datetime.datetime.strptime(string_format,format)
    if time_format:
        return datetime.datetime.strftime(time_format,format)

#start_date must be the format '%Y-%m-%d:%H:%M' and its type must be string
def add_date(date_strp=None,date_strf=None,hours=24,minutes=60):
    format='%Y-%m-%d:%H:%M'
    if date_strf:
       #convert the type of the start_date into datetime type
       date_strp=date_conversion(format,string_format=date_strf)
    
    #add random numbers to the initial date
    h,m=np.random.randint(0,hours),np.random.randint(0,minutes)
    end_date=date_strp+datetime.timedelta(hours=h,minutes=m)
    return end_date


def create_session():
    uid_=str(uuid.uuid4())
    temp=re.match('([\w]+)-[\w]+-[\w]+-[\w]+-([\w]+)',uid_)
    short_session,long_session=temp.group(1),temp.group(2)
    return short_session,long_session




def assign_subject(service_code,is_partial=False):
    info,front,category=main_category['info'],main_category['front'],main_category['category']
    subject_code_front=np.random.choice(info['subject_codes_front'],p=info['subject_codes_pr'])
    code_front=front[subject_code_front]
    alpha_front,start,end=code_front[0],code_front[1],code_front[2]
    code_name=alpha_front+'0'+str(np.random.randint(start,end+1))

    if code_name not in category[subject_code_front]:
        raise SubjectCodeError
        
    if is_partial:
        return code_name
    
    record=category[subject_code_front][code_name]
    price=record['price']
    if service_code=='bk_ck':
        price/=10
    product_code=subject_code_front+alpha_front+code_name
    return product_code,price
    
def generate_code(service_code,courses):
    code,price=assign_subject(service_code)
    if code in courses:
        generate_code(service_code,courses)
    else:
        courses.add(code)
        return code,price,courses
   
def create_productcode(service_code,courses):
    num_items=np.random.randint(1,3)
    product_code=''
    total=0
    
    for _ in range(num_items):
        code,price=assign_subject(service_code)
        try:
            code,price,courses=generate_code(service_code,courses)
            if not product_code:
                product_code+=code
            else:
                product_code=product_code+','+code
            total+=price
        except:
            pass
    return product_code,total,courses



def create_urls(lang='eng',event='After'):
    keys=['source','medium','campaign','term']

    elements={}
    for key in keys:
        if key=='campaign' or key=='term':
            if key=='term':
                elements[key]=np.random.choice(urls[key][lang][event])
            else:
                elements[key]=np.random.choice(urls[key]['lists'])
        else:
            elements[key]=np.random.choice(urls[key]['lists'],p=urls[key]['pr'])
           
    source,medium,campagin,term=elements['source'],elements['medium'],elements['campaign'],elements['term']
    return f'https://www.visangstudy.com?utm_source={source}&utm_medium={medium}&utm_campaign={campagin}&utm_term={term}'


def create_final_path():
    step_path=np.random.choice(urls['path']['second_path'])
    if step_path!='/':
        step_path=step_path.format(np.random.randint(4500,5000),np.random.randint(1,10))
        step_path+=np.random.choice(urls['path']['third_path'])
    return step_path

def create_path(age):
    info=main_category['info']
    path=''
    start_t=urls['path']['start']
    start=np.random.choice(start_t['lists'],p=start_t['pr'])
  
    if start!='binary_asp':
        service_code=np.random.choice(info['service_codes'],p=info['service_pr'])
        level=urls['search_type']['difficulty']
        difficulty=np.random.choice(level['lists'],p=level['pr'])
        serach_type=assign_subject(service_code,is_partial=True)+'-'+urls['search_type']['level'][age]+'-'+difficulty
        return start+'?search_='+serach_type
    if start=='search_list':
        path+='search_list/search_input'
    else:
        path+=start
    middle=np.random.choice(urls['path']['teacher_v2'],p=[0.7,0.3])
    path=path+'/teacher_v2'+middle+create_final_path()
    return path


def create_session():
    uid_=str(uuid.uuid4())
    temp=re.match('([\w]+)-[\w]+-[\w]+-[\w]+-([\w]+)',uid_)
    short_session,long_session=temp.group(1),temp.group(2)
    return short_session,long_session






def create_record(params,courses):
    assert type(params)==dict,'parameters must be dict-type'
    product_code,amount,courses=create_productcode(params['service_code'],courses)
    url,path=create_urls(lang=params['lang'],event=params['event']),create_path(params['age'])
    
    record={
        # 'unique_key':str(uuid.uuid4()),``
        'user_id':params['user_id'],
        'short_session':params['short_session'],
        'long_session':params['long_session'],
        'url':url,
        'path':path,
        'action':params['action'],
        'transaction':{
            'product_code':product_code,\
            'amount':amount,
            'time_stamp':params['time_stamp']
        }
    }


    # #include values to be inlcuded in the final record
    # keys=['action','short_session','long_session','user_id','time_stamp']
    # for key in keys:
    #   record[key]=params[key]

    return record,product_code,courses
    


def generate_cart_transaction(add_cart,user_id):
    session={}
    cart_transaction=[]
    cnts=0
    while add_cart:
        chosen_date=np.random.choice(list(add_cart.keys()))
        if not add_cart[chosen_date]:
            add_cart.pop(chosen_date)
            continue
        mins_=np.random.randint(1,len(add_cart[chosen_date])+1)
        products=[add_cart[chosen_date].pop() for _ in range(mins_)]
        if not products:
            break
        product_code=''
        price=0
        try:
            for product in products:
                front,subject_code=product[0],product[1:3]+product[-2:]
                price+=main_category['category'][front][subject_code]['price']
                if not product_code:
                    product_code+=product
                else:
                    product_code=product_code+','+product
        except:
            pass
        
        new_strf=date_conversion('%Y-%m-%d:%H:%M',time_format=add_date(date_strf=chosen_date))
        new_strf_ymd=re.match('[0-9]{4}-[0-9]{1,2}-[0-9]{1,2}',new_strf)
        if new_strf_ymd and new_strf_ymd not in session:
            short_session,long_session=create_session()
            session[new_strf_ymd]=(short_session,long_session)
        else:
            short_session,long_session=session[new_strf_ymd]
        
        action=np.random.choice(['purchase','remove_cart'])
        
        user_id_temp=user_id+'-'+'{}'.format(cnts)
        record={
            # 'unique_key':str(uuid.uuid4()),
            'user_id':user_id,
            'short_session':short_session,
            'long_session':long_session,
            'url':'https://visanstudy.com/me',
            'path':'/cart',
            'action':action,
            'transaction':{
                'product_code':product_code,
                'amount':price,
                'time_stamp':new_strf
            }
        }    
        cnts+=1
        cart_transaction.append(record)
    return cart_transaction
     
        


def make_transacitons(user,register_date,age,time_plus=True,session_limit_hours=48,lang='eng',event='After'):
    results=[]
    if time_plus:
        start_strp=add_date(date_strf=register_date)
    else:
        start_strp=date_conversion('%Y-%m-%d:%H:%M',string_format=register_date)

    add_cart=dict()
    iterations=np.random.choice(1,3)
    actions=urls['actions']
    service_code=np.random.choice(service_code_tuple[0],p=service_code_tuple[1])
    courses=set()
    for iter,_ in enumerate(iterations):
        action=np.random.choice(actions['category'],p=actions['pr'])
        trans_strpt=add_date(date_strp=start_strp,hours=session_limit_hours)
        if int((trans_strpt-start_strp).days)>0 or iter==0:
            short_session,long_session=create_session()
        params={
            'action':action,
            'service_code':service_code,
            'age':age,
            'lang':lang,
            'event':event,
            'short_session':short_session,
            'long_session':long_session,
            'user_id':user,
            'time_stamp':date_conversion(format='%Y-%m-%d:%H:%M',time_format=trans_strpt)
             }
        record,product_code,courses=create_record(params=params,courses=courses)
        results.append(record)
        if action=='add_cart':
            trans_strft=date_conversion(format='%Y-%m-%d:%H:%M',time_format=trans_strpt)
            add_cart[trans_strft]=product_code.split(',')
    
    if add_cart:
        results+=generate_cart_transaction(add_cart,user)
    return results


def load_sql_data(query):
    conn=pymysql.connect(
        host=dc_config('student_info_host'),
        port=int(dc_config('student_info_port')),
        user=dc_config('student_info_user'),
        database=dc_config('student_info_database'),
        password=dc_config('student_info_password'),
        use_unicode=True,
        charset='utf8')

    cursor=conn.cursor(pymysql.cursors.DictCursor)
    cursor.execute(query)
    data=cursor.fetchall()
    conn.close()
    return data




class SQLLoadError(ValueError):
    def __init__(self,msg='Table name is rquired'):
        super().__init__(msg)
        
        

        
def main(config):
    
    if config.sql_db:
        try:
            if config.sql_table:
                query='SELECT * FROM {}'.format(config.sql_table)
                records=load_sql_data(query)
        except:
            raise SQLLoadError 

    if config.json_input:
        with open(config.json_input,'r',encoding='UTF-8') as f:
            records=json.load(f)

    if config.output_path:
        data=[]

        if config.is_logging:
                file_path=os.path.join(config.output_path,'transaction.log')
                f=open(file_path,'a')
        for record in enumerate(records):
            try:
                user_info=make_transacitons(user=record['user_id'],register_date=record['register_date'],age=record['age'],lang='kor')
            except:
                record=record[1]
                user_info=make_transacitons(user=record['user_id'],register_date=record['register_date'],age=record['age'],lang='kor')
        
            for info in user_info:
                #save the data in the format of log file
                if config.is_logging:
                    
                    #specify the file name
                    file_name='loginApp.log' if not config.file_name else config.file_name

                    utc_now=datetime.datetime.strftime(datetime.datetime.utcnow(),"%Y%m%d")
                    #Login status is simply categorizsed as either SUCESS OR ERROR
                    random_number=np.random.randint(12345,123253)
                    status='SUCCESS'  if np.random.binomial(1,0.968,1)==1 else 'ERROR'

                    user_id,short_session,long_session=info['user_id'],info['short_session'],info['long_session']
                    action,url,path=info['action'],info['url'],info['path']
                    transaction=','.join([f'{key}:{value}'  for key,value in info['transaction'].items()])
                    info=f'/usr/transaction/data/demo/{utc_now}/{file_name}:{random_number}:[[]]-UserId:{user_id}=Action:{action} {status} ShortSession:{short_session} LongSession:{long_session} [loginApp] VisangEduDuo transaction:[{transaction}]-URL:{url} Path:{path}'
                    f.write(info+'\n')
                else:
                    data.append(info)
        
 
        if config.is_logging:
            f.close()
        else:
            data=pd.DataFrame.from_dict(data)
            dt=datetime.datetime.utcnow().strftime("%Y-%m-%d")
            new_folder="{}\{}".format(config.output_path,dt)
            try:
                os.mkdir(new_folder)
            except:
                pass
            os.chdir(new_folder)
            data.to_parquet('transaction.parquet',engine='pyarrow',
                            compression='snappy')
            
            

   





if __name__=='__main__':
    config=argparse_transaction()
    main(config)






