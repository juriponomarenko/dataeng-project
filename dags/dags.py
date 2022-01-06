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
sys.path.append('/opt/airflow/dags/util/sql_analysis')
sys.path.append('/opt/airflow/dags/util/mongodb_analysis')
sys.path.append('/opt/airflow/dags/util/machine_learning')
sys.path.append('/opt/airflow/dags/util/neo_ingestion')
sys.path.append('/opt/airflow/dags/util/neo_analysis')


import util.kym_cleaning as kc
import util.kym_spotlight_cleaning as ksc
import util.kym_vision_cleaning as kvc
import util.sql_ingestion_query as sql_ingest
import util.mongodb_analysis as mongodb_analysis
import util.sql_analysis as sql_analysis
import util.machine_learning as ml_analysis
import util.neo_ingestion as neo_ingestion
import util.neo_analysis as neo_analysis

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
        return 'connection_ok'
    except:
        return 'connection_fails'


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

    df_all.to_json('/opt/airflow/dags/data/kym_vs.json', orient='records')


def _persisit_mongodb_data():

    client = MongoClient('mongodb://mongodb:27017/', username='airflow', password='airflow')

    db_name = 'project'
    collection_name = 'kym'

    if db_name in client.list_database_names():
        client.drop_database(db_name)

    db = client[db_name]

    collection = db.create_collection(collection_name)

    with open('/opt/airflow/dags/data/kym_vs.json') as f:
        kym_json = json.load(f)

    collection.insert_many(kym_json)


def _report_fail():
    with open("/opt/airflow/dags/data/.dag_output_report.txt", "w") as f:
        f.write(
            "DAG progress output:\n" 
            "internet connection failed\n"
        )
        f.close()


def _report_success():
    with open("/opt/airflow/dags/data/.dag_output_report.txt", "w") as f:
        f.write(
            "DAG progress output:\n" 
            "SQL analysis succeeded and its output is sql_analysis_*.json\n"
            "Pymongo analysis succeeded and its output is mongo_analysis_result.json \n"
            "ML analysis succeeded and its output is ml_analysis_result_kym_vs2.json\n"
            "Neo4j analysis succeeded and its output is neo_analysis_*.json\n"
        )
        f.close()


connection_check = BranchPythonOperator(
    task_id='connection_check',
    dag=project_dag,
    python_callable=_connection_check,
    op_kwargs={},
    trigger_rule='all_success',
)

connection_fails = DummyOperator(
    task_id='connection_fails',
    dag=project_dag,
    trigger_rule='all_success',
    depends_on_past=False,
)

connection_ok = DummyOperator(
    task_id='connection_ok',
    dag=project_dag,
    trigger_rule='all_success',
    depends_on_past=False
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
    sql='data/ingestion_q.sql',
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

run_mongodb_analysis = PythonOperator(
    task_id='run_mongodb_analysis',
    dag=project_dag,
    python_callable=mongodb_analysis.run_mongodb_analysis,
    op_kwargs={},
    trigger_rule='all_success',
    depends_on_past=False,
)

run_sql_analysis = PythonOperator(
    task_id='run_sql_analysis',
    dag=project_dag,
    python_callable=sql_analysis.run_sql_analysis,
    op_kwargs={},
    trigger_rule='all_success',
    depends_on_past=False,
)

run_machine_learning_analysis = PythonOperator(
    task_id='run_machine_learning_analysis',
    dag=project_dag,
    python_callable=ml_analysis.run_ml_analysis,
    op_kwargs={},
    trigger_rule='all_success',
    depends_on_past=False,
)

#it takes time (about 40min)
insert_data_to_neo = PythonOperator(
    task_id='insert_data_to_neo',
    dag=project_dag,
    python_callable=neo_ingestion.run_neo_ingestion,
    op_kwargs={},
    trigger_rule='all_success',
    depends_on_past=False,
)

run_neo_analysis = PythonOperator(
    task_id='run_neo_analysis',
    dag=project_dag,
    python_callable=neo_analysis.run_neo_analysis,
    op_kwargs={},
    trigger_rule='all_success',
    depends_on_past=False,
)


report_success = PythonOperator(
    task_id='report_success',
    dag=project_dag,
    python_callable=_report_success,
    op_kwargs={},
    trigger_rule='all_success',
    depends_on_past=False,
)

report_fail = PythonOperator(
    task_id='report_fail',
    dag=project_dag,
    python_callable=_report_fail,
    op_kwargs={},
    trigger_rule='all_success',
    depends_on_past=False,
)


create_data_dir >> connection_check >> [connection_ok, connection_fails]

connection_fails >> report_fail

connection_ok >> [download_kym_spotlight, download_kym, download_kym_vision, prepare_sql_schema]

download_kym_spotlight >> clean_kym_spotlight

download_kym >> clean_kym

download_kym_vision >> clean_kym_vision

[clean_kym_spotlight, clean_kym, clean_kym_vision, prepare_sql_schema] >> enrichment

enrichment >> [prepare_sql_ingestion_query, insert_data_to_mongodb, run_machine_learning_analysis,insert_data_to_neo]

prepare_sql_ingestion_query >> insert_data_to_sql_db

insert_data_to_sql_db >> run_sql_analysis

insert_data_to_mongodb >> run_mongodb_analysis

insert_data_to_neo >> run_neo_analysis

[run_mongodb_analysis, run_sql_analysis, run_machine_learning_analysis, run_neo_analysis] >> report_success
