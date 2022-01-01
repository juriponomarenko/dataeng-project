import pandas as pd
import numpy as np

def clean():

    df = pd.read_json('/opt/airflow/dags/data/kym_spotlight.json')
    df = df.T.reset_index()

    # we consider only "resources"
    df = df[['index', 'Resources']].rename(columns={'index': 'title'})

    # title as url ending
    df['title'] = df.apply(lambda row: row['title'].split('memes/')[1].replace('-', ' ').lower(), axis=1)

    # Drop if Resources are not present
    ind_res_notna = df['Resources'].notna()
    df = df.loc[ind_res_notna]

    # drop duplicates
    df = df.drop_duplicates(["title"])

    # collect DBPedia resources URI endings if similarityScore is high
    resur = []

    for res in df['Resources']:
        resr = []
        for resi in res:
            if float(resi['@similarityScore']) > 0.99:
                r = resi['@URI'].split('resource/')[1].replace('-', ' ').replace('_', ' ').lower()
                resr.append(r)
        resur.append(np.unique(np.array(resr)))

    df['DBPedia_resources'] = resur

    # add number of resources
    df['DBPedia_resources_n'] = df['DBPedia_resources'].apply(lambda x: len(x))

    # finalising
    df_final = df.drop(['Resources'], axis=1)
    df_final.to_json("/opt/airflow/dags/data/kym_spotlight_cleaned.json", orient='records')
