from pymongo import MongoClient
import pandas as pd


def run_mongodb_analysis():

    # connect to collection
    client = MongoClient('mongodb://mongodb:27017', username='airflow', password='airflow')
    db = client["project"]

    collection = db["kym"]

    # find meme with most resources
    meme = list(collection.find().sort([("DBPedia_resources_n", -1)]).limit(1))[0]

    # find titles (memes) with strongest intersection between DBPedia_resources with most_res
    # (memes not related directly but having something in common)
    pipeline = [
        # make collection project with title etc and commonTags
        {'$project':
             {'title': 1,
              'parent': 1,
              'children': 1,
              'siblings': 1,
              'DBPedia_resources': 1,
              'commonRes': {'$setIntersection': ['$DBPedia_resources', meme['DBPedia_resources']]}
              }
         },
        # leave only long commonTags arrays
        {'$match':
             {'commonRes.2': {'$exists': True}}  # commonTags.2 is third element on array
         },
        # check if close relationships are missing
        {'$match':
             {'parent': {'$exists': True},  # parent exist
              'parent': {'$ne': ''},  # parent not null
              'parent': {'$ne': meme['parent']},  # parents not match
              'title': {'$nin': meme['children']},  # selected is not most_res's child
              'children.0': {'$exists': True},  # children exist and not null
              'children': {'$elemMatch': {"$nin": [meme['title']]}}  # children do not contain most_res's title
              }
         }
    ]

    result = list(collection.aggregate(pipeline))[0]

    # save result
    df = pd.read_json('/opt/airflow/dags/data/kym_vs.json')
    ind = (df['title'] == meme['title']) | (df['title'] == result['title'])
    df.loc[ind].to_json("/opt/airflow/dags/data/mongo_analysis_result.json", orient='records')
