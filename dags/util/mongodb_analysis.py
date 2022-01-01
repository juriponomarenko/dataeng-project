from pymongo import MongoClient
import pandas as pd
import json

#connect to collection
myclient = MongoClient('mongodb://airflow_mongodb_1:27017/', username='airflow',password='airflow')  #airflow_mongodb_1 is mongo container name in docker ps
mydb = myclient["project"]
kym_vs_coll = mydb["kym_vs"]

#find meme with most resources
for i in kym_vs_coll.find().sort([("DBPedia_resources_n",-1)]).limit(1):
    most_res=i

#find titles (memes) with strongest intersection between DBPedia_resources with most_res 
#(memes not related directly but having something in common)
pipeline=[
    #make collection project with title etc and commonTags
    { '$project': 
         {'title': 1, 
          'parent':1,
          'children':1,
          'siblings':1,
          'DBPedia_resources':1,
          'commonRes': {'$setIntersection': ['$DBPedia_resources', most_res['DBPedia_resources']]}
         } 
    },
    #leave only long commonTags arrays
    { '$match': 
         {'commonRes.2' : {'$exists': True} } #commonTags.2 is third element on array
    },
    #check if close relationships are missing
    { '$match':
         {'parent':{'$exists': True},  #parent exist
          'parent':{'$ne': ''},      #parent not null
          'parent': {'$ne': most_res['parent']},  #parents not match
          'title': {'$nin': most_res['children']},  #selected is not most_res's child
          'children.0':{'$exists': True},  #children exist and not null
          'children': {'$elemMatch': {"$nin":[most_res['title']]}}  #children do not contain most_res's title
        }
    }
]

for i in kym_vs_coll.aggregate(pipeline):
    result=i

#save result
df=pd.read_json('/opt/airflow/dags/project/data/kym_vs.json')
ind=(df['title']==most_res['title']) | (df['title']==result['title'])  
df.loc[ind].to_json("/opt/airflow/dags/project/data/mongo_analysis_result.json",orient='records')
