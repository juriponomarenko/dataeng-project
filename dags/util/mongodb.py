from pymongo import MongoClient
import json

myclient = MongoClient('mongodb://airflow_mongodb_1:27017/', username='airflow',password='airflow')  #airflow_mongodb_1 is mongo container name in docker ps
mydb = myclient["project"]

#create collection
#kym_vs_coll.drop()
kym_vs_coll = mydb["kym_vs"]

#insert data
with open('/opt/airflow/dags/project/data/kym_vs.json') as f:
    kym_json = json.load(f)

kym_vs_coll.insert_many(kym_json)