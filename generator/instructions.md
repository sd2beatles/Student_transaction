## 0. EC2 시작

```sh
pip3 install --upgrade awscli
aws configure 
aws configure list
```



## 1.시작 

generate_student_info.py는 local에서 시작하여,s3 bucket으로 보내기로 한다. 그러므로 access_key,secret key를 이미 aws configure를 통해서 설정해놨다
면 bash script에 추가적으로 적시할 필요는 없다. 
generate_student_info.py(local) -----> s3(trigger)----> lambda(transactions_lambda) ----> EC2(student_transactions.py) \
---> s3 ----> lambda(producer) --->dynamodb 

## 2. lambda(transactions_lambda) 설정


주의할 점, lambda funcion 설정 및 paramiko module을 lambda layer에 적재할 때는, 파이썬 버전을 무조건 3.6으로 맞춘다.
아니면, backend_error가 발생할 수 있다. 
```
I've faced with the same issue on python 3.7 (cffi==1.11.2, cryptography==2.1.2, paramiko==2.3.1) and 
resolved it downgrading to python 3.6.
```

```python
pip3 install virtualenv 
python3 -m virtualenv temp
source temp/bin/activate
mkdir lambda_layers
cd lambda_layers
mkdir python
cd python
pip install paramiko -t ./
cd ..
zip -r python_modules.zip .

aws s3 cp "./python_modules.zip" s3://<folder-name>/
```
![image](https://user-images.githubusercontent.com/53164959/129483145-e8564112-3522-421a-bc3d-26baf28e39d3.png)
![image](https://user-images.githubusercontent.com/53164959/131804821-d5ddfd65-f0a4-411b-956c-fac2be11e734.png)

layers를 설정한 다음에는 다시 labmbda함수로 다시 돌아와서, layer를 추가한다.
![image](https://user-images.githubusercontent.com/53164959/129483198-d203cc97-e71e-47e3-9ecb-fef4fbed8850.png)
![image](https://user-images.githubusercontent.com/53164959/129483209-d7453dac-45bb-40ee-97a6-250e8aa099f1.png)


## 3. s3 trigger
이제 s3 trigger를 설정해야 한다. 이때 주의할 점은 bucket name을 꼭 확인해야한다는 것이다. 우리의 data source 이름은
aws-david-test 이고 이미 s3에 bucket에 만들어져 있어야 한다. 

![image](https://user-images.githubusercontent.com/53164959/129483963-c7f15e6e-1886-4355-8192-1ae4afdbe579.png)

![image](https://user-images.githubusercontent.com/53164959/129484028-33aa56a9-446b-4669-b782-e5ca552f3506.png)

Triggger을 설정했다면, 다음에는 ec2 instance를 설정하러 가보자.

## 4. ec2 instance 
ec2 instance 설정 후에, vs code로 들어간다. visual studio code를 통해서, ec2 접속해 보자. 


![image](https://user-images.githubusercontent.com/53164959/129484528-30f80dbc-2e7e-48d1-8695-d1df9a4cba76.png)

이 때, host name은 public ipv4이며, pem file을 받은 후에 저장소를 지정해주면 된다. 
![image](https://user-images.githubusercontent.com/53164959/129484551-552ba4d4-32c6-4869-9082-c670f9c5a558.png)

다음을 클릭한 후에, 접속해 보자.
![image](https://user-images.githubusercontent.com/53164959/129484580-0abca374-1f13-4ee5-a152-a54eec443ca3.png)

화며니 접속되었다면, python 관련 libararies 설치해야한다. 
                                                                                                      
 ```sh
 sudo apt update
 sudo apt install python3-pip
 pip3 install discord.py
 
 ```
 
 crt+shft+p 를 통해서 python selector를 설정해준다. 그리고는 project folder를 하나 만들고, requirements.txt 파일에 설치할 libraries이름을
 열거한다. 
 
 ```txt
pyarrow==0.16.0
pandas 
numpy
scipy
backoff_utils
awswrangler
```
안되면, 수동으로 설치해준다.
```sh
pip install cython
python3 -m pip install --system -r requirements.txt -t ./libs

```
awscli를 설정해주자 

```sh
pip3 install --upgrade awscli
sudo apt install awscli
aws configure
```

![image](https://user-images.githubusercontent.com/53164959/129487205-2b34ef97-cd7d-4a4c-ae6b-41e54a6ff4a3.png)

파이썬 코드를 작성한 후 , bash로 집어넣는다. 임의적으로 이 bash를 launch_project.sh 라고 칭하겠다.
```sh
python3 generate_transactions.py --s3_bucket_source $1 \
--s3_key_source $2 \
--s3_bucket_target aws-david-test \
--s3_folder_target student_transactions 
```

## 5. s3 trigger 와  transaction_lambda를 엮어주기 

s3를 들어가서 원하는 bucket_name을 선택한 후 properties을 클릭.

![image](https://user-images.githubusercontent.com/53164959/129488962-967aef1b-5ff0-41a9-ba64-3de8dbd312b3.png)

create event notification을 선택해준다. 

![image](https://user-images.githubusercontent.com/53164959/129488969-5a1b444b-3a8b-4884-a789-1272b9206f62.png)

여기서 정말로 중요한데...

Event name 임의적으로 아무거나 설정

Prefix : 만약 david-aws-test 안에 폴더가 student-info 와 student-trans라는 두 가지 폴더가 존재한다고 가정하자.그리고 
         우리는 source folder를 student-info로 설정하고 싶다면 stduent-info/ 꼭 명시하는 것을 잊지말자. 
        
Event types : All object로 설정


Destination: lambda 설정을 누르고, 상응하는 함수 선택 

save changes를 누르고, 다시 lambda 함수로 들어가면. 둘이 엮여 있는 것을 확인. 



```sh

python3 generate_transactions.py --s3_bucket_source $1 \
--s3_key_source $2 \
--s3_bucket_target aws-david-test \
--s3_folder_target student_transactions 

```

## 6. lambda 함수 full


```pythoon

import json
import paramiko
import boto3
import urllib
import sys
def lambda_handler(event, context):
    try:
     bucket_source = event['Records'][0]['s3']['bucket']['name']
     key_source = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')

    
    except Exception as e:
      print(e)
      print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
      raise e
    
    print(bucket_source,key_source)
    aws_access_key_id,aws_secret_access_key='AKIAT6YZYML3NUPVGGU3','tpQlhrWohmzO+6xJp/gUMO7Dgp58ZSJ+gznDNMin'
    s3=boto3.client('s3',aws_access_key_id=aws_access_key_id,aws_secret_access_key=aws_secret_access_key)
    username='ubuntu'
   
    ec2 = boto3.resource('ec2',aws_access_key_id=aws_access_key_id,aws_secret_access_key=aws_secret_access_key)
    instance_id='i-0cc6caf1096ed6da4'
    instances = ec2.instances.filter(Filters=[{'Name': 'instance-state-name', 'Values': ['running']}])
    for instance in instances:
        if instance.id==instance_id:
            p2_instance=instance
            break
    
    bucket_name='david-pem'
    key_name='david.pem'
    key_location='/tmp/david.pem'
    s3.download_file(bucket_name,key_name,key_location)
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    pkey = paramiko.RSAKey.from_private_key_file(key_location)
    ssh.connect(instance.public_dns_name,username=username,pkey=pkey)
    cmd_to_run='cd /home/ubuntu/project && sh launch_project.sh {} {}'.format(str(bucket_source),str(key_source))
    stdin4,stdout4,stderr5=ssh.exec_command(cmd_to_run,get_pty=False)
    print('success')

```

## 7. Kinesis Data Stream

먼저 data stream을 설정한다. 
![image](https://user-images.githubusercontent.com/53164959/129494377-21ebf5e1-4954-4f5c-a30e-addcfc6a4ec5.png)

 
s3(trigger) ---->lambda producer(매개체) ----> kinesis data stream 

7.1 먼저 s3 trigger(s3-->bucket--> Create event notification)

![image](https://user-images.githubusercontent.com/53164959/129494464-e04ffa4e-a74d-4a48-bf37-8a54f4089d42.png)
![image](https://user-images.githubusercontent.com/53164959/129494469-4f6da01e-5f44-44e5-a8b9-eebc1e3ce543.png)


7.2 lambda producer로 이동 
```python
import json
import urllib.parse
import uuid
import boto3
import time
import random
import datetime
from botocore.retries import bucket

class InputDataError(TypeError):
    def __init__(self,msg='Check your input type.') -> None:
        super().__init__(msg)


class KinesisProducer:
  def __init__(self,data,streamName,batch_size=100,maximum_records=None):
    self.batch_size=min(batch_size,1000)
    self.maximum_records=maximum_records
    self.data_length=len(data)
    self.data=data
    self.streamName=streamName
    #Here we careate a new session per thread 
    #and create a reource client using our thread's session object
    self.kinesis_client=boto3.session.Session().client('kinesis')
  

  @staticmethod
  def get_kinesis_record(record):
    try:
      '''
      Generate an item with a random hash key on a large range, and a unique sort key, and  a created date
      '''
      item={'hashKey':random.randrange(0,5000000),'sortKey':str(uuid.uuid4()),
            'created': datetime.datetime.utcnow().isoformat()}
      raw_data=json.dumps({**record,**item},ensure_ascii=False)
      encoded_data=bytes(raw_data,encoding = "utf-8")
      kinesis_record={
        'Data':encoded_data,
        'PartitionKey':str(item['hashKey'])
      }
      return kinesis_record
    except:
      raise InputDataError
      
  def generate_and_submit(self):
    while self.data_length>0:
      batch_length=self.batch_size if self.data_length>=self.batch_size else self.data_length
      records_batch=[self.get_kinesis_record(self.data.pop())  for _ in range(batch_length)]
      
      request={
        'Records':records_batch,
        'StreamName':self.streamName
      }
      print(request)
      response=self.kinesis_client.put_records(**request)
     
      self.submit_batch_until_sucessful(records_batch,response)
      self.data_length-=batch_length
      print('Batch inserted. Total records: {}'.format(str(batch_length)))
    return 



  def submit_batch_until_sucessful(self,batch,response):
     """ If needed, retry a batch of records, backing off exponentially until it goes through"""
     retry_interval = 0.5
     
     failed_record_count = response['FailedRecordCount']

     while failed_record_count:
        time.sleep(retry_interval)

        # Failed records don't contain the original contents - we have to correlate with the input by position
        batch= [batch[i] for i, record in enumerate(response['Records']) if 'ErrorCode' in record]

        print('Incrementing exponential back off and retrying {} failed records'.format(str(len(batch))))
        retry_interval = min(retry_interval * 2, 10)
        request = {
            'Records': batch,
            'StreamName':self.streamName
        }

        result = self.kinesis_client.put_records(**request)
        failed_record_count = result['FailedRecordCount']



    
    
s3 = boto3.client('s3')

def lambda_handler(event, context):
  try:
     bucket_name = event['Records'][0]['s3']['bucket']['name']
     key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
     response=s3.get_object(Bucket=bucket_name,Key=key)
     content=response['Body']
     data=json.loads(content.read())

  except Exception as e:
      print(e)
      print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
      raise e

  kinesis=KinesisProducer(data,'workflow_data_stream')
  kinesis.generate_and_submit()
  print("kinesis producer has successfully finshed")
 ```
 
 
 7.3 lamabdas consumer 작성 및 kinesis datastream 과 엮어주기 
 
 
 ```python
 
 
 import base64
import json
import boto3
import datetime
from decimal import Decimal



def lambda_handler(event, context):
    """
    Receive a batch of events from Kinesis and insert into our DynamoDB table
    """
    print('Received request')
    item = None
    dynamo_db = boto3.resource('dynamodb')
    table = dynamo_db.Table('demo_workflow_data')
    decoded_record_data = [base64.b64decode(record['kinesis']['data']) for record in event['Records']]
    #If the input contains float values,make sure that all float values be converted into Decimla type
    #Otherwise, dynamodb raises TypeError. One way to around it is to use parse_float in json.loa
    deserialized_data = [json.loads(decoded_record,parse_float=Decimal) for decoded_record in decoded_record_data]

    with table.batch_writer() as batch_writer:
        for item in deserialized_data:
            # Add a processed time so we have a rough idea how far behind we are
            item['processed'] = datetime.datetime.utcnow().isoformat()
            batch_writer.put_item(Item=item)

    # Print the last item to make it easy to see how we're doing
    print(json.dumps(item))
    print('Number of records: {}'.format(str(len(deserialized_data))))
 ```
 
 ![image](https://user-images.githubusercontent.com/53164959/129494552-ad004207-ec6e-4d36-879d-8e51a2bd825c.png)

7.4 Create dynamo db

![image](https://user-images.githubusercontent.com/53164959/129494579-d129bd3b-5ce0-46a8-8657-f8f985211c55.png)
![image](https://user-images.githubusercontent.com/53164959/129494620-d0ada87a-8f7f-4f6e-9a8e-b15532b3e20c.png)

![image](https://user-images.githubusercontent.com/53164959/129496788-d7969ede-413e-4107-8593-5a34c17ba096.png)

