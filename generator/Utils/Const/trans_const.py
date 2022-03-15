# Modify the paramter settings at your disposal 
# Modify the paramter settings at your disposal 

main_category={
        'info':{
        'service_codes':['tr_lt','bk_ck','ent_info'],
        'service_name':['teacher_lecture','bookmall','Admission Info'],
        'service_pr':[0.65,0.25,0.1],
        'subject_codes_front':['a','b','c','d','e'],
        'subject_codes_pr':[0.35,0.25,0.1,0.2,0.1]
        },
        'front':{'a':('mt',1,7),'b':('en',1,4),'c':('kr',1,6),'d':('sc',1,5),'e':('ss',1,5),'f':('as',1,5)},
        'category':{'a':{
                'mt01':{'name':'Math1','price':292000},
                'mt02':{'name':'Math2','price':254000},
                'mt03':{'name':'Statistics','price':145000},
                'mt04':{'name':'Calculas','price':350000},
                'mt05':{'name':'Avanced Calculas','price':400000},
                'mt06':{'name': 'Geometric','price':350000},
                'mt07':{'name':'Internal(Math)','price':250000}
                },
                'b':{
                    'en01':{'name':'Reading(EN)','price':105000},
                    'en02':{'name':'Listening','price':105000},
                    'en03':{'name':'Grammar','price':105000},
                    'en04':{'name':'Internal(EN)','price':200000}
                    },
                'c':{
                    'kr01':{'name':'Reading(KR)','price':105000},
                    'kr02':{'name':'Literature','price':105000},
                    'kr03':{'name':'Language and Media','price':105000},
                    'kr04':{'name':'Narration and Grammar','price':200000},
                    'kr05':{'name':'Introductory Course(KR)','price':325000},
                    'kr06':{'name':'Internal(kR)','price':230000}
                    },
                'd':{
                        'sc01':{'name':'Physics','price':154000},
                        'sc02':{'name':'Chemistry','price':160000},
                        'sc03':{'name':'Biology','price':170000},
                        'sc04':{'name':'Earth Science','price':180000},
                        'sc05':{'name':'Introductary(SS)','price':200000}
                        },
                'e':{
                            'ss01':{'name':'Geography(KOR)','price':150000},
                            'ss02':{'name':'Geography(WR)','price':124000},
                            'ss03':{'name':'Moral&Ethic','price':134000},
                            'ss04':{'name':'Politics&Law','price':150000},
                            'ss05':{'name':'Economics','price':200000}
                        }
                    }
            }
       



terms={
    'eng':{'Before':
           ['Internal assessment','on-line course','English','Korean','Free trials','on-line',
           'Improved grade','course site','Science','Speech and Writting','Grammar','Reading','Literature',
           'physics','Chmestry','Biology','politics and Law','Economics','Probability and statistics',
           'Korean Geography'],
           'After':
            ['internal assessment','on-line course','English','Korean','Free trials','on-line',
           'improved grade','course site','Science','Geometric and Vectors','Unlimited access','Biology'
           'special price offer','test practices','early admission','CSAT','Text book','full packages',
           'preparation for university entrance','Korean','Chemstry-2','Grammar','Caculus','Calculus 2',
           'Basic Course for Mathmatics','Probability and statistics','Differentiation','Integeration',
           'Easy Acess to Mathmatics','Introductory Math Courses for totally Beginners','Functional Skills Math Level',
           'Advanced Caclualus','The complete course on Math Fundamental','Pysics','Chemstry','Math course for Lower Grades',
           'Entry-level Math for freshmen']
           },
    'kor':{'Before':
              ['내신','수능','온라인 강좌','영어','국어','무료 온라인','온라인 강의',\
               '성적향상','강의 사이트','과학','기화와 벡터 온라인 강의',\
               '무제한 수강','환급 강의','모의고사','수시','정시','대학별 고사','온라인 고재','전영역 강의',\
               '모의 고사 준비','논술','지구과학','미적분','쉬운 수학강의','확률과 통계',\
               '수학의 쉬운 접근','초짜를 위한 수학','수학 스킬강좌','상위권 수학','물리'],
            'After':
            ['내신','수능','온라인 강좌','영어','국어','무료 온라인','온라인 강의',\
               '성적향상','강의 사이트','과학','기화와 벡터 온라인 강의',\
               '무제한 수강','환급 강의','모의고사','수시','정시','대학별 고사','온라인 고재','전영역 강의',\
               '모의 고사 준비','논술','지구과학','미적분','쉬운 수학강의','확률과 통계',\
               '수학의 쉬운 접근','초짜를 위한 수학','수학 스킬강좌','상위권 수학','물리','기하','수학1','수학2',
               '4-5등급을 위한 수학','중학수학','지구과학','생물','미적분 1','고급 수학','예비고등을 위한 입문 수학']}
        }



urls={
    'actions':{
    'category':['add_cart','view','purchase'],
    'pr':[0.6,0.2,0.2]},
    'source':{'lists':[
          'https://www.youtube.com/watch',\
          'htts://www.facebook.com',\
          'https://www.twitter.com',\
          'https://www.instagram.com',\
          'https://www.daum.net/',\
          'https://www.naver.com/',\
          'https://www.kakao.com',\
          'https://www.zum.com/',\
          'https://www.africatv.com/',\
          'https://www.danawa.com'],
          'pr':[0.05,0.05,0.05,0.1,0.2,0.3,0.1,0.05,0.05,0.05]},
    'medium':{'lists':['cpc','banner','email','tv','telemarketing','text'],'pr':[0.1,0.5,0.1,0.1,0.05,0.15]},
    'campaign':{'lists':['semeter_start_sale','package_promotion','short_promotion','senior_promotion',\
                         'monthly_promotion']},
    'term':terms,
    'search_type':{
              'level':{14:'m1',15:'m2',16:'m3',17:'g1',18:'g2',19:'g3'},
              'difficulty':{'lists':['advance','intermediate','moderate','fundamental'],'pr':[0.1,0.2,0.3,0.4]}
            },
    'path':{
        'start':{'lists':['/search_list','/search_input','/binary_asp'],'pr':[0.2,0.3,0.5]},
        'teacher_v2':['/t_promotion','/chr_detail'],
        'second_path':['/lecture_detailview.asp?CHR_CD={}&&Make_FLG={}','/'],
        'third_path':['/member/member_login.asp','/']
        }
    }
    
