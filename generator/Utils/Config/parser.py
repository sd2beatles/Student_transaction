import argparse    
import multiprocessing
from xmlrpc.client import Boolean

def argparse_student_info():
    p=argparse.ArgumentParser()
    p.add_argument('--language',type=str,default='en')
    p.add_argument('--year',required=True)
    p.add_argument('--month',required=True)
    p.add_argument('--day_start',type=int,default=1)
    p.add_argument('--day_end',type=int,default=30)
    p.add_argument('--hours_start',type=int,default=0)
    p.add_argument('--hours_end',type=int,default=24)
    p.add_argument('--minutes_start',type=int,default=0)
    p.add_argument('--minutes_end',type=int,default=60)
    p.add_argument('--data_length',type=int,required=True)
    p.add_argument('--multiprocessing_on',type=int,default=0)
    p.add_argument('--core_count',type=int,default=multiprocessing.cpu_count())
    p.add_argument('--output_path',type=str)
    p.add_argument('--db_info',type=bool)
    p.add_argument('--s3_full_path',type=str)
    p.add_argument('--s3_upload_date',type=str,default='now')
    config = p.parse_args()
    return config


def argparse_study_hours():
    p=argparse.ArgumentParser()
    p.add_argument('--event_period',type=int,required=True)
    p.add_argument('--input_path',type=str,required=True)
    p.add_argument('--output_path',type=str,required=True)
    p.add_argument('--key',type=str,required=True)
    p.add_argument('--multiprocessing_on',type=bool,default=True)
    p.add_argument('--core_nums',type=int,default=multiprocessing.cpu_count())
    parser=p.parse_args()
    return parser


def argparse_transaction():
    p=argparse.ArgumentParser()
    p.add_argument('--sql_db',type=Boolean)
    p.add_argument('--s3_path',type=str)
    p.add_argument('--json_input',type=str)
    p.add_argument('--output_path',type=str)
    p.add_argument('--sql_table',type=str)
    p.add_argument('--is_logging',type=Boolean)
    p.add_argument('--file_name',type=str)
    parser=p.parse_args()
    return parser
