# 학원 수강학생 관련 거래 데이터 생성,적제 및 모델링 

## 1) Objective

전 직장에서 자주 봤던 회사 내부의 로그파일들의 유사한 형태로 데이터를 생성시켜, 이를 토대로 주어진 공간에 적재 및 모델링을 하는 것이 주 목표입니다.

## 2) Tool Sets

- Operating System: WSL2 Unbuntu 
- Program Language: Python3.8
- Big Data Tool: Pyspark 3.0.1
- Workflow: Apache Airflow
- Cloud System: AWS (S3,Dynamodb,Athena)
- Docker
- Jupyter Notebook


## 3) Method

### 3.1 Generate Student Transaction

학생들의 각 각의 자료들은 크게 세 가지로 구분되어 있습니다.

    - 수강생의 인적 사항     
    - 수강생의 수강 구매 기록 
    - 수강생의 온라인 로그인 기록
    
  각각의 자료를 생성할 수 있는 simulator를 generator 폴더 안에 저장되어 있습니다. 각 생성된 자료들은 내부 디스크 또는 aws s3로 전달될 수 있게 설정을
  해놓았습니다. 더 자세한 정보는 /home/david/airflow/generator/Utils/Config/parser.py을 살펴보시면 될거 같습니다. 예를 들어 내부 디스크 안에 
  기록을 저장하고 싶다면,아래와 같이 설정하시면 됩니다. 만약 생성된 자료가 많을 시에는 병렬처리 프로그램도 설정을 해놓았기 때문에,cpu의 갯수와 적절한
  config를 설정하시면 <b>Parallelism</b>으로 데이터의 생성 속도를 빠르게 증진시킬 수 있습니다. 
  
  ```sh

  python generate_transactions.py \
--json_input    <input_space> \
--output_path   <output_path> --is_logging True
  ```
 위의 명령으로 인해 다음과 같은 출력을 얻을 수 있습니다. 
 
  ```txt
  /usr/transaction/data/demo/20220312/loginApp.log:72393:[[]]-UserId:nross=Action:add_cart SUCCESS ShortSession:e57b8710 LongSession:91b3d5bb2b31 [loginApp] VisangEduDuo transaction:[product_code:ckrkr05,amtmt06,amount:675000,time_stamp:2022-01-24:04:33]-URL:https://www.visangstudy.com?utm_source=htts://www.facebook.com&utm_medium=banner&utm_campaign=monthly_promotion&utm_term=논술 Path:/search_list?search_=mt03-m1-fundamental

/usr/transaction/data/demo/20220312/loginApp.log:70543:[[]]-UserId:nross=Action:add_cart SUCCESS ShortSession:6228dd42 LongSession:a9719df6e03f [loginApp] VisangEduDuo transaction:[product_code:benen01,amtmt02,amount:359000,time_stamp:2022-01-22:19:44]-URL:https://www.visangstudy.com?utm_source=https://www.twitter.com&utm_medium=cpc&utm_campaign=semeter_start_sale&utm_term=모의 고사 준비 Path:/search_list?search_=sc02-m1-intermediate
```
  
  ### 3.2 Load and Processed Data 
  
 이렇게 축척된 데이터를 s3 또는 내부에 지정된 곳에 저장하셨다면, raw data를 전처리를 해야합니다. 이때, airflow를 이용하며, 각각의 task가 잘 진행되는지
 체크할 수 있습니다. 
 
 ![image](https://user-images.githubusercontent.com/53164959/158401328-3a56c52d-b24d-4e65-be74-36f4a2982e5c.png)
 
 
 
 ### 4. Data Ananysis 
 
Pysparkf를 통해서 두 번째 전처리를 진행한 후에, 데이터 분석을 진행합니다. 자세한 내용은 transaction_pyspark.ipynb를 확인해 주세요. 

![image](https://user-images.githubusercontent.com/53164959/158402041-737670b7-df34-424d-8b8e-af9783ce90c5.png)


### 5. Modeling 

마지막으로 Modeling을 통해서, 각 학생의 평균적인 로그인 시간(즉 수강시간)을 예측해 봅니다. 자세한 내용은 transaction_modeling.ipynb를 확인해 주세요. 



### 6. Verision 

아직까지 첫 버전이라서 오류가 많을거라 사료됩니다. 일을 하면서 만든  개인 적인 포트폴리오라 시간도 많이 들었고, 서툼점도 많습니다. 하지만 이것이 끝이 아니라는 지속적인 update를 통해서 더 나은 버전을 만들어 보겠습니다. 긴 글 읽어주셔서 감사합니다. 

  

  
  
 
