import numpy as np

scores=[8.1,14.1,23.4,40.1,50.4]
total=np.sum(scores)
ratio=[s/total  for s in scores]
grades=['A','B','C','D','E']
is_online_ratio=[0.7,0.3]
satisfaction_ratio=[0.45,0.35,0.2]
no_interest_ratio=[0.2,0.6,0.2]

params={
    'A':{'alpha':20,'beta':5,'is_online':1,'offline_course':[0,1],'satisfaction_last':[0,1,2],'no_interest':[3,4,5],'possion':[30,5],'competition_factor':0.95},
    'B':{'alpha':13,'beta':5,'is_online':1,'offline_course':[1,2,3],'satisfaction_last':[1,2,3],'no_interest':[4,3,2],'possion':[20,1],'competition_factor':1.2},
    'C':{'alpha':10,'beta':7,'is_online':1,'offline_course':[0,1,2],'satisfaction_last':[2,3,4],'no_interest':[3,2,1],'possion':[10,1],'competition_factor':2.5},
    'D':{'alpha':5,'beta':7,'is_online':None,'offline_course':[0,1],'satisfaction_last':[0,1,2],'no_interest':[0,1,2],'possion':[2,1],'competition_factor':0.6},
    'E':{'alpha':4,'beta':10,'is_online':None,'offline_course':[0,1],'satisfaction_last':[0,1,2],'no_interest':[1,0,2],'possion':[1,1],'competition_factor':0.2}
}
