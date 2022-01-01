import pandas as pd
import numpy as np


def clean():

    df = pd.read_json('/opt/airflow/dags/data/kym.json')

    # leaving only meme
    df = df[df.category == "Meme"]

    # remove duplicates
    df = df.drop_duplicates(["title", "url", "last_update_source"])

    # keep only latest updated
    df = df.sort_values("last_update_source")
    df = df.drop_duplicates(["title", "url"], keep="last")

    # dates to datetime format, NaN to 0001-01-01
    dates = pd.to_datetime(df.added, unit="s")
    df['year_added'] = dates.dt.date
    df[['year_added']] = df[['year_added']].fillna(value='0001-01-01')
    df['year_added'] = df['year_added'].astype('str')  # otherwise json decode back to s

    # title as url ending withoout '-' and lowercase
    df['title'] = df.apply(lambda row: row['url'].split('memes/')[1].replace('-', ' ').lower(), axis=1)

    # same for parent, siblings, children
    ind_par_notna = df['parent'].notna()
    df.loc[ind_par_notna, 'parent'] = df.loc[ind_par_notna, 'parent'].apply(
        lambda x: x.split('memes/')[1].replace('-', ' ').lower())
    ind_sib_notna = df['siblings'].notna()
    df.loc[ind_sib_notna, 'siblings'] = df.loc[ind_sib_notna, 'siblings'].apply(
        lambda arr: [x.split('memes/')[1].replace('-', ' ').lower() for x in arr])
    ind_chi_notna = df['children'].notna()
    df.loc[ind_chi_notna, 'children'] = df.loc[ind_chi_notna, 'children'].apply(
        lambda arr: [x.split('memes/')[1].replace('-', ' ').lower() for x in arr])

    # for parent/siblings/children NaN put empty array []
    df[['parent']] = df[['parent']].fillna(value='unknown')
    ind_sib_isna = df['siblings'].isna()
    df.loc[ind_sib_isna, 'siblings'] = df.loc[ind_sib_isna, 'siblings'].apply(lambda x: [])
    ind_chi_isna = df['children'].isna()
    df.loc[ind_chi_isna, 'children'] = df.loc[ind_chi_isna, 'children'].apply(lambda x: [])
    ind_kw_isna = df['search_keywords'].isna()
    df.loc[ind_kw_isna, 'search_keywords'] = df.loc[ind_kw_isna, 'search_keywords'].apply(lambda x: [])
    # fixing index
    df = df.reset_index(drop=True)

    print(df.columns)
    # get necessary columns
    df = df[['title', 'url', 'year_added', 'meta', 'details', 'tags', 'parent', 'siblings', 'children', 'search_keywords']]

    # get description from "meta"
    descs = []

    for meta in df.meta:
        if "description" in meta:
            v = meta["description"]
        else:
            v = np.NaN
        descs.append(v)

    df["description"] = descs

    # drop NaN description (only 16)
    df = df.dropna(subset=['description'])

    # get origin and year from details
    origs = []
    years = []

    for det in df.details:
        if "origin" in det:
            orig = det["origin"].replace('\'', '').lower()
            if orig == None:
                orig = np.NaN
        else:
            orig = np.NaN
        if "year" in det:
            year = det["year"]
            if year == None:
                year = np.NaN
        else:
            year = np.NaN
        origs.append(orig)
        years.append(year)

    df["origin"] = origs
    df["year"] = years

    # year NaN to 0001-01-01
    df[['year']] = df[['year']].fillna(value='0001')

    # format year into yyyy-mm-dd
    df['year'] = df['year'].apply(lambda x: x + '-01-01')

    # enrichment
    df = df.reset_index(drop=True)
    df['tags_n'] = df['tags'].apply(lambda x: len(x))
    df['siblings_n'] = df['siblings'].apply(lambda x: len(x))
    df['children_n'] = df['children'].apply(lambda x: len(x))
    df['description_n'] = df['description'].apply(lambda x: len(x))

    # finalise
    df_final = df.drop(['meta', 'details'], axis=1)
    df_final = df_final[
        [
            'title', 'url', 'year_added', 'tags', 'tags_n', 'parent',
            'siblings', 'siblings_n', 'children', 'children_n',
            'description', 'description_n', 'origin', 'year',
            'search_keywords'
         ]
    ]
    df_final.to_json("/opt/airflow/dags/data/kym_cleaned.json", orient='records')
