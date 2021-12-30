import datetime
import urllib.request as request
import pandas as pd

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


default_args_dict = {
    #'start_date': airflow.utils.dates.days_ago(0),
    'start_date': datetime.datetime(2021, 12, 29, 14, 40, 0),
    'concurrency': 1,
    'schedule_interval': None,
    #'schedule_interval': "0 0 * * *",  #runs once a week
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
}

project_dag = DAG(
    dag_id='project_dag',
    default_args=default_args_dict,
    template_searchpath='/opt/airflow/dags/data',
    catchup=False,                        
)

#----------------------------------------------
def _connection_check():
    try:
        request.urlopen('https://meme4.science')
        return 'data_dir'
    except:
        return 'report'

connection = BranchPythonOperator(
    task_id='connection',
    dag=project_dag,
    python_callable=_connection_check,
    op_kwargs={
    },
#    trigger_rule='all_success',
)

report = DummyOperator(
    task_id='report',
    dag=project_dag,
    trigger_rule='all_success',
    depends_on_past=False,
)
#-------------------------------------

data_dir = BashOperator(
    task_id='data_dir',
    dag=project_dag,
    bash_command="mkdir /opt/airflow/dags/data",
    trigger_rule='all_success',
    depends_on_past=False,
)

installs = BashOperator(
    task_id='installs',
    dag=project_dag,
    bash_command="pip install ijson pymongo",
    trigger_rule='all_success',
    depends_on_past=False,
)

down_kym = BashOperator(
    task_id='down_kym',
    dag=project_dag,
    bash_command="curl -o /opt/airflow/dags/data/kym.json https://owncloud.ut.ee/owncloud/index.php/s/g4qB5DZrFEz2XLm/download/kym.json",
    trigger_rule='all_success',
    depends_on_past=False,
)

down_s = BashOperator(
    task_id='down_s',
    dag=project_dag,
    bash_command="curl -o /opt/airflow/dags/data/kym_spotlight.json https://owncloud.ut.ee/owncloud/index.php/s/iMM8crN4AKSpFZZ/download/kym_spotlight.json",
    trigger_rule='all_success',
    depends_on_past=False,
)

down_v = BashOperator(
    task_id='down_v',
    dag=project_dag,
    bash_command="curl -o /opt/airflow/dags/data/kym_vision.json https://owncloud.ut.ee/owncloud/index.php/s/teoFdWKBzzqcFjY/download/kym_vision.json",
    trigger_rule='all_success',
    depends_on_past=False,
)

clean_kym = BashOperator(
    task_id='clean_kym',
    dag=project_dag,
    bash_command='python /opt/airflow/dags/util/kym_cleaning.py',
    trigger_rule='all_success',
    depends_on_past=False,
    )

clean_s = BashOperator(
    task_id='clean_s',
    dag=project_dag,
    bash_command='python /opt/airflow/dags/util/kym_spotlight_cleaning.py',
    trigger_rule='all_success',
    depends_on_past=False,
    )

clean_v = BashOperator(
    task_id='clean_v',
    dag=project_dag,
    bash_command='python /opt/airflow/dags/util/kym_vision_cleaning.py',
    trigger_rule='all_success',
    depends_on_past=False,
    )



#-------------------------------------------
def _enrichment():
    kym=pd.read_csv('/opt/airflow/dags/data/kym_cleaned.csv')
    kymv=pd.read_csv('/opt/airflow/dags/data/kym_vision_cleaned.csv')
    pd1=kym.merge(kymv, on="title")
    pd1[['origin']] = pd1[['origin']].fillna(value='unknown')
    kyms=pd.read_csv('/opt/airflow/dags/data/kym_spotlight_cleaned.csv')
    df_all=pd1.merge(kyms, how='left', on='title')
    ind_db_isna=df_all['DBPedia_resources'].isna()
    df_all.loc[ind_db_isna,'DBPedia_resources'] = df_all.loc[ind_db_isna,'DBPedia_resources'].apply(lambda x:[])
    df_all[['DBPedia_resources_n']] = df_all[['DBPedia_resources_n']].fillna(value=0)
    df_all[['DBPedia_resources_n']]=df_all[['DBPedia_resources_n']].astype('int')
    df_all.to_csv("/opt/airflow/dags/data/kym_vs.csv", index=False)

enrichment = PythonOperator(
    task_id='enrichment',
    dag=project_dag,
    python_callable=_enrichment,
    op_kwargs={
    },
    trigger_rule='all_success',
    depends_on_past=False,
    )
#-------------------------------------------
def _ingestion_query():
    df = pd.read_csv('/opt/airflow/dags/data/kym_vs.csv')
    with open("/opt/airflow/dags/data/ingestion_query.sql", "w") as f:
        df_iterable = df.iterrows()
        f.write(
            "CREATE TABLE IF NOT EXISTS kym_vs (\n" 
            "title VARCHAR(255),\n"
            "year_added DATE,\n"
            "tags_n INTEGER,\n"
            "parent VARCHAR(255),\n"
            "siblings_n INTEGER,\n"
            "children_n INTEGER,\n"
            "description_n INTEGER,\n"
            "origin VARCHAR(255),\n"
            "year DATE,\n"
            "adult VARCHAR(255),\n"
            "spoof VARCHAR(255),\n"
            "medical VARCHAR(255),\n"
            "violence VARCHAR(255),\n"
            "racy VARCHAR(255),\n"
            "label VARCHAR(255),\n"
            "dbpedia_resources_n INTEGER);\n"
        )
        for index, row in df_iterable:
            title = row['title']
            year_added = row['year_added']
            tags_n = row['tags_n']
            parent = row['parent']
            siblings_n = row['siblings_n']
            children_n = row['children_n']
            description_n = row['description_n']
            origin=row['origin']
            year = row['year']
            adult = row['adult']
            spoof = row['spoof']
            medical = row['medical']
            violence = row['violence']
            racy = row['racy']
            label = row['label']
            resources_n = row['DBPedia_resources_n']	
            f.write(
                "INSERT INTO kym_vs VALUES ("
                f"'{title}', '{year_added}', {tags_n}, '{parent}', {siblings_n}, {children_n}, {description_n}, '{origin}', '{year}', '{adult}', '{spoof}', '{medical}', '{violence}', '{racy}', '{label}', {resources_n}"
                ");\n"
            )
        f.close()


ingestion_query = PythonOperator(
    task_id='ingestion_query',
    dag=project_dag,
    python_callable=_ingestion_query,
    op_kwargs={
    },
    trigger_rule='all_success',
#    depends_on_past=False,
)        

ingestion_sql = PostgresOperator(
    task_id='ingestion_sql',
    dag=project_dag,
    postgres_conn_id='postgres_default',
    sql='ingestion_query.sql',
#    sql='/data/ingestion_query.sql',
    trigger_rule='all_success',
    autocommit=True,
)

#-------------------------------------------
def _analysis_sql():
#	query = 'analysis_year_diff.sql'
    query1="WITH diff AS (SELECT EXTRACT(year FROM year_added)-EXTRACT(year FROM year) AS d FROM kym_vs WHERE year_added>'0001-01-01' AND year>'0001-01-01') SELECT COUNT(d) counts, d difference FROM diff GROUP BY d ORDER BY d;"
    query2="WITH years AS (SELECT EXTRACT(year FROM year) AS y FROM kym_vs WHERE year>'0001-01-01') SELECT COUNT(y) counts, y FROM years GROUP BY y ORDER BY counts DESC;"
    query3="SELECT title,children_n FROM kym_vs ORDER BY children_n DESC;"
    query4="SELECT parent,COUNT(siblings_n) counts FROM kym_vs GROUP BY parent ORDER BY counts DESC;"
    query5="SELECT COUNT(title) counts,origin FROM kym_vs GROUP BY origin ORDER BY counts DESC;"
    query6="WITH ad AS (SELECT title,parent, adult FROM kym_vs WHERE adult='likely' OR adult='possible') SELECT COUNT(title) counts, parent FROM ad GROUP BY parent ORDER BY counts DESC;"
    query7="WITH ad AS (SELECT title,origin,adult FROM kym_vs WHERE adult='likely' OR adult='possible') SELECT COUNT(title) counts, origin FROM ad GROUP BY origin ORDER BY counts DESC;"
    hook = PostgresHook(postgres_conn_id="postgres_default")
    df = hook.get_pandas_df(sql=query1)
    df.to_csv("/opt/airflow/dags/data/analysis_year_diff.csv", index=False)
    df = hook.get_pandas_df(sql=query2)
    df.to_csv("/opt/airflow/dags/data/analysis_year_breakthrough.csv", index=False)
    df = hook.get_pandas_df(sql=query3)
    df.to_csv("/opt/airflow/dags/data/analysis_most_children.csv", index=False)
    df = hook.get_pandas_df(sql=query4)
    df.to_csv("/opt/airflow/dags/data/analysis_most_children_via_siblings.csv", index=False)
    df = hook.get_pandas_df(sql=query5)
    df.to_csv("/opt/airflow/dags/data/analysis_most_popular_origin.csv", index=False)
    df = hook.get_pandas_df(sql=query6)
    df.to_csv("/opt/airflow/dags/data/analysis_most_adult_oriented_parent.csv", index=False)
    df = hook.get_pandas_df(sql=query7)
    df.to_csv("/opt/airflow/dags/data/analysis_most_productive_origin_for_adult_oriented_memes.csv", index=False)


analysis_sql = PythonOperator(
    task_id='analysis_sql',
    dag=project_dag,
    python_callable=_analysis_sql,
    op_kwargs={
    },
    trigger_rule='all_success',
#    depends_on_past=False,
)        


end = DummyOperator(
    task_id='end',
    dag=project_dag,
    trigger_rule='none_failed'
)


connection >> [report, data_dir]

report >> end

data_dir >> installs >> [down_s,down_kym,down_v]

down_s >> clean_s

down_kym >> clean_kym

down_v >> clean_v

[clean_s, clean_kym,clean_v] >> enrichment >> ingestion_query >> ingestion_sql

ingestion_sql >> analysis_sql
