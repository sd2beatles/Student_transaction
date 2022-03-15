from scipy.stats import poisson,expon
import datetime
import numpy as np
import argparse
import json
from Errors.errors import GradeError,InputDataError
from Utils.Config.parser import argparse_study_hours
from Utils.Config.shared_method import *
import multiprocessing
from Utils.Const import log_info
import os


log_info=log_info.log_transactions


def make_study_hours(grade):
    range_=log_info['study_hours'][grade]['range']
    low_=np.random.choice(range_,p=log_info['study_hours'][grade]['pr'])
    x=np.random.uniform(low_,low_+0.1)
    y=expon.ppf(x,loc=log_info['study_hours'][grade]['loc'],scale=log_info['study_hours'][grade]['scale'])
    return int(y)

def compute_time_interval(grade,start,hours,minutes,days=0,only_end=False):
    h,m=np.random.randint(hours[0],hours[1]),np.random.randint(minutes[0],minutes[1])
    start=start+datetime.timedelta(days=days,hours=h,minutes=m)
    
    ms=make_study_hours(grade)
    end=start+datetime.timedelta(minutes=ms)
    if only_end:
        return end
    else:
        return start,end

def generate_interval_days(grade,key):
    x_=np.random.uniform(log_info[key][grade]['poisson_start'],log_info[key][grade]['poisson_end'])
    y=poisson.pmf(x_,mu=log_info[key][grade]['mu'],loc=log_info[key][grade]['loc'])
    return int(y)


        
def generate_attendents(grade,key):
    lower_limit=ord(grade)
    upper_limit=1 if grade=='A' else log_info[key][chr(lower_limit-1)]['percent']
    return np.random.uniform(log_info[key][grade]['percent'],upper_limit)


def date_conversion(stamp):
    return datetime.datetime.strftime(stamp,'%Y-%m-%d:%H:%M')
        
    

def login_trail_period(id_,grade,initial_date,key,event_period,first_login=True):
    result=[]
    initial_strp=datetime.datetime.strptime(initial_date,'%Y-%m-%d:%H:%M')
    asci_=ord(grade)
    if asci_<65 or asci_>69:
        raise GradeError

    if first_login:
        days_interval=generate_interval_days(grade,'first_login')
        #generate the date of first login
        first_login_strp=initial_strp+datetime.timedelta(days=days_interval)
    else:
        first_login_strp=initial_strp

    #compute number of attendents 
    props=generate_attendents(grade,key)
    days_attendents=int(event_period*props)

    #schedule the stduy hour a user spends right after login
    absents=event_period-days_attendents
    absents_days=sorted(np.random.choice(event_period+1,int(absents),replace=False))
    
    for i in range(1,event_period+1):
        device=str(np.random.choice(['web','app'],p=[0.3,0.7]))
        if i in absents_days:
            continue
        login_stamp,logout_stamp=compute_time_interval(grade,hours=[17,22],minutes=[0,55],start=first_login_strp,days=i)
        
        
        result.append({'id':id_,'login_timestamp':date_conversion(login_stamp),
                       'logout_timestamp':date_conversion(logout_stamp),'device':device})
        for j in range(0,log_info[key][grade]['freq']+1):  
            login_stamp,logout_stamp=compute_time_interval(grade,hours=[1,2],minutes=[0,30],start=logout_stamp)
            result.append({'id':str(id_),'login_timestamp':str(date_conversion(login_stamp)),
                           'logout_timestamp':str(date_conversion(logout_stamp)),'device':device})
    return result   

'''
The goal of generating transactions is to check if there is 
any change in a student's login behavior after a given event. 
We assume that a list of students' information is already acquired from the previous step. 
To produce artificial online transactions, We have several parsers to specify to create 
'artificial' transactions that are as follows; 

event-period: the period you will schedule a ceratin event for 
input-path: the path that currently holds information on members
output-path: the path to which you will save the final data 
key: We have three keys to choose from (during_event,event_after)

'''

def create_group_record(users,parser,core_index,start,finish):
   
    inner_results=[]
    print('{} core starts'.format(core_index))
    for index in range(start,finish):
        member=users[index]
        record=login_trail_period(member['user_id'],member['grade'],member['register_date'],parser.key,parser.event_period)
        inner_results.append(record)
    print('{} core ends'.format(core_index))
    return inner_results


def single_core(users,parser,start,end):
    inner_results=list()
    for index in range(start,end):
        member=users[index]
        record=login_trail_period(member['user_id'],member['grade'],member['register_date'],parser.key,parser.event_period)
        inner_results.append(record)
    return inner_results

def main(parser):
    global users
    trans=[]
    data_size=len(users)
    if parser.multiprocessing_on:
        cores=min(multiprocessing.cpu_count(),parser.core_nums)
        
        merged_results,remain=multiprocessing_task(users,parser,cores,data_size,create_group_record)
        if remain>0:
            merged_results+=single_core(users,parser,(data_size-remain),data_size)
        merged_results=sum(merged_results,[])

    else:
        trans+=single_core(users,parser,0,data_size)
    
    data=pd.DataFrame.from_dict(merged_results)
    dt=datetime.datetime.utcnow().strftime("%Y-%m-%d")
    new_folder="{}\{}".format(parser.output_path,dt)

    try:
        os.mkdir(new_folder)
    except:
        pass
    os.chdir(new_folder)
    data.to_parquet('study_hours.parquet',engine='pyarrow',
                    compression='snappy')

if __name__=='__main__':
    parser=argparse_study_hours()
    with open(parser.input_path,'r') as f:
        users=json.load(f)
    main(parser)
