import datetime
import json
import sys
import urllib.request as request
import pandas as pd

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from pymongo import MongoClient

sys.path.append('/opt/airflow/dags/util/kym_cleaning')
sys.path.append('/opt/airflow/dags/util/kym_spotlight_cleaning')
sys.path.append('/opt/airflow/dags/util/kym_vision_cleaning')
sys.path.append('/opt/airflow/dags/util/sql_ingestion_query')

import util.kym_cleaning as kc
import util.kym_spotlight_cleaning as ksc
import util.kym_vision_cleaning as kvc
import util.sql_ingestion_query as sql_ingest

default_args_dict = {
    'start_date': datetime.datetime.now(),
    'concurrency': 1,
    'schedule_interval': None,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
}

project_dag = DAG(
    dag_id='project_dag',
    default_args=default_args_dict,
    template_searchpath='/opt/airflow/dags/data',
    catchup=False,                        
)


def _connection_check():
    try:
        request.urlopen('https://meme4.science')
        return 'create_data_dir'
    except:
        return 'end'


def _enrichment():
    kym = pd.read_json('/opt/airflow/dags/data/kym_cleaned.json', encoding='utf-8')
    kymv = pd.read_json('/opt/airflow/dags/data/kym_vision_cleaned.json', encoding='utf-8')
    pd1 = kym.merge(kymv, on="title")
    pd1[['origin']] = pd1[['origin']].fillna(value='unknown')
    kyms = pd.read_json('/opt/airflow/dags/data/kym_spotlight_cleaned.json', encoding='utf-8')
    df_all = pd1.merge(kyms, how='left', on='title')
    ind_db_isna = df_all['DBPedia_resources'].isna()
    df_all.loc[ind_db_isna, 'DBPedia_resources'] = df_all.loc[ind_db_isna, 'DBPedia_resources'].apply(lambda x: [])
    df_all[['DBPedia_resources_n']] = df_all[['DBPedia_resources_n']].fillna(value=0)
    df_all[['DBPedia_resources_n']] = df_all[['DBPedia_resources_n']].astype('int')

    df_all.to_json('/opt/airflow/dags/data/kym_vs.json')


def _prepare_sql_query():
    df = pd.read_json('/opt/airflow/dags/data/kym_vs.json', encoding='utf-8')


def _persisit_mongodb_data():

    client = MongoClient('mongodb://mongodb:27017/', username='airflow', password='airflow')

    db_name = 'project'
    collection_name = 'kym'

    if db_name in client.list_database_names():
        client.drop_database(db_name)

    db = client[db_name]

    collection = db.create_collection(collection_name)

    with open('/opt/airflow/dags/data/kym_vs.json') as f:
        kym_json = pd.read_json(f)

    collection.insert_many(kym_json)


connection = BranchPythonOperator(
    task_id='connection',
    dag=project_dag,
    python_callable=_connection_check,
    op_kwargs={
    },
#    trigger_rule='all_success',
)


create_data_dir = BashOperator(
    task_id='create_data_dir',
    dag=project_dag,
    bash_command="mkdir -p /opt/airflow/dags/data && chmod a+rwx -R /opt/airflow/dags/data",
    trigger_rule='all_success',
    depends_on_past=False,
)

download_kym = BashOperator(
    task_id='download_kym',
    dag=project_dag,
    bash_command="curl -o /opt/airflow/dags/data/kym.json https://owncloud.ut.ee/owncloud/index.php/s/g4qB5DZrFEz2XLm/download/kym.json",
    trigger_rule='all_success',
    depends_on_past=False,
)

download_kym_spotlight = BashOperator(
    task_id='download_kym_spotlight',
    dag=project_dag,
    bash_command="curl -o /opt/airflow/dags/data/kym_spotlight.json https://owncloud.ut.ee/owncloud/index.php/s/iMM8crN4AKSpFZZ/download/kym_spotlight.json",
    trigger_rule='all_success',
    depends_on_past=False,
)

download_kym_vision = BashOperator(
    task_id='download_kym_vision',
    dag=project_dag,
    bash_command="curl -o /opt/airflow/dags/data/kym_vision.json https://owncloud.ut.ee/owncloud/index.php/s/teoFdWKBzzqcFjY/download/kym_vision.json",
    trigger_rule='all_success',
    depends_on_past=False,
)

clean_kym = PythonOperator(
    task_id='clean_kym',
    dag=project_dag,
    python_callable=kc.clean,
    trigger_rule='all_success',
    depends_on_past=False,
    )

clean_kym_spotlight = PythonOperator(
    task_id='clean_kym_spotlight',
    dag=project_dag,
    python_callable=ksc.clean,
    trigger_rule='all_success',
    depends_on_past=False,
    )

clean_kym_vision = PythonOperator(
    task_id='clean_kym_vision',
    dag=project_dag,
    python_callable=kvc.clean,
    trigger_rule='all_success',
    depends_on_past=False,
    )

enrichment = PythonOperator(
    task_id='enrichment',
    dag=project_dag,
    python_callable=_enrichment,
    op_kwargs={},
    trigger_rule='all_success',
    depends_on_past=False,
    )

prepare_sql_schema = PostgresOperator(
    task_id='prepare_sql_schema',
    dag=project_dag,
    postgres_conn_id='postgres_default',
    sql='schema.sql',
    trigger_rule='all_success',
    autocommit=True,
    depends_on_past=False,
)

prepare_sql_ingestion_query = PythonOperator(
    task_id='prepare_sql_ingestion_query',
    dag=project_dag,
    python_callable=sql_ingest.generate_sql_ingestion_query,
    op_kwargs={},
    trigger_rule='all_success',
    depends_on_past=False,
)

# takes 3-5 minutes depending on your machine
insert_data_to_sql_db = PostgresOperator(
    task_id='insert_data_to_sql_db',
    dag=project_dag,
    postgres_conn_id='postgres_default',
    sql='ingestion_q.sql',
    trigger_rule='all_success',
    autocommit=True,
    depends_on_past=False,
)

insert_data_to_mongodb = PythonOperator(
    task_id='insert_data_to_mongodb',
    dag=project_dag,
    python_callable=_persisit_mongodb_data,
    op_kwargs={},
    trigger_rule='all_success',
    depends_on_past=False,
)


# #-------------------------------------------
# def _analysis_sql():
# #	query = 'analysis_year_diff.sql'
#     query1="WITH diff AS (SELECT EXTRACT(year FROM year_added)-EXTRACT(year FROM year) AS d FROM kym_vs WHERE year_added>'0001-01-01' AND year>'0001-01-01') SELECT COUNT(d) counts, d difference FROM diff GROUP BY d ORDER BY d;"
#     query2="WITH years AS (SELECT EXTRACT(year FROM year) AS y FROM kym_vs WHERE year>'0001-01-01') SELECT COUNT(y) counts, y FROM years GROUP BY y ORDER BY counts DESC;"
#     query3="SELECT title,children_n FROM kym_vs ORDER BY children_n DESC;"
#     query4="SELECT parent,COUNT(siblings_n) counts FROM kym_vs GROUP BY parent ORDER BY counts DESC;"
#     query5="SELECT COUNT(title) counts,origin FROM kym_vs GROUP BY origin ORDER BY counts DESC;"
#     query6="WITH ad AS (SELECT title,parent, adult FROM kym_vs WHERE adult='likely' OR adult='possible') SELECT COUNT(title) counts, parent FROM ad GROUP BY parent ORDER BY counts DESC;"
#     query7="WITH ad AS (SELECT title,origin,adult FROM kym_vs WHERE adult='likely' OR adult='possible') SELECT COUNT(title) counts, origin FROM ad GROUP BY origin ORDER BY counts DESC;"
#     hook = PostgresHook(postgres_conn_id="postgres_default")
#     df = hook.get_pandas_df(sql=query1)
#     df.to_csv("/opt/airflow/dags/data/analysis_year_diff.csv", index=False)
#     df = hook.get_pandas_df(sql=query2)
#     df.to_csv("/opt/airflow/dags/data/analysis_year_breakthrough.csv", index=False)
#     df = hook.get_pandas_df(sql=query3)
#     df.to_csv("/opt/airflow/dags/data/analysis_most_children.csv", index=False)
#     df = hook.get_pandas_df(sql=query4)
#     df.to_csv("/opt/airflow/dags/data/analysis_most_children_via_siblings.csv", index=False)
#     df = hook.get_pandas_df(sql=query5)
#     df.to_csv("/opt/airflow/dags/data/analysis_most_popular_origin.csv", index=False)
#     df = hook.get_pandas_df(sql=query6)
#     df.to_csv("/opt/airflow/dags/data/analysis_most_adult_oriented_parent.csv", index=False)
#     df = hook.get_pandas_df(sql=query7)
#     df.to_csv("/opt/airflow/dags/data/analysis_most_productive_origin_for_adult_oriented_memes.csv", index=False)
#
#
# analysis_sql = PythonOperator(
#     task_id='analysis_sql',
#     dag=project_dag,
#     python_callable=_analysis_sql,
#     op_kwargs={
#     },
#     trigger_rule='all_success',
# #    depends_on_past=False,
# )


end = DummyOperator(
    task_id='end',
    dag=project_dag,
    trigger_rule='none_failed'
)


connection >> [end, create_data_dir]

create_data_dir >> [download_kym_spotlight, download_kym, download_kym_vision, prepare_sql_schema]

download_kym_spotlight >> clean_kym_spotlight

download_kym >> clean_kym

download_kym_vision >> clean_kym_vision

[clean_kym_spotlight, clean_kym, clean_kym_vision, prepare_sql_schema] >> enrichment

enrichment >> [prepare_sql_ingestion_query, insert_data_to_mongodb]

prepare_sql_ingestion_query >> insert_data_to_sql_db
