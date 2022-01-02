from airflow.hooks.postgres_hook import PostgresHook


def run_sql_analysis():
    prefix = '/opt/airflow/dags/data'
    hook = PostgresHook(postgres_conn_id="postgres_default")

    df_dim_counts = hook.get_pandas_df("""
        SELECT 
            title, tags_count, children_count, keywords_count 
        FROM 
            meme_fact 
        GROUP BY title, tags_count, children_count, keywords_count
        ORDER BY tags_count + children_count + keywords_count desc;
    """)

    df_memes_distribution_by_year = hook.get_pandas_df("""
        SELECT 
            COUNT(*), dd.year
        FROM (
            SELECT DISTINCT 
                title, date_id 
            FROM meme_fact
        ) mf
        JOIN date_dim dd ON dd.id = mf.date_id
        GROUP BY dd.year
        ORDER BY dd.year ASC;
    """)

    df_memes_distribution_by_month_name = hook.get_pandas_df("""
        SELECT 
            COUNT(*), dd.month_name
        FROM (
            SELECT DISTINCT 
                title, date_id 
            FROM meme_fact
        ) mf
        JOIN date_dim dd on dd.id = mf.date_id
        GROUP BY dd.month_name, dd.month_number
        ORDER BY dd.month_number ASC;
    """)

    df_memes_distribution_by_origin = hook.get_pandas_df("""
        SELECT COUNT(*), origin FROM (
            SELECT DISTINCT 
                title, url, origin_id 
            FROM meme_fact
        ) mf
        JOIN origin_dim o ON o.id = mf.origin_id
        GROUP BY o.origin
        ORDER BY count DESC;
    """)

    df_origins_with_most_adult_memes = hook.get_pandas_df("""
        SELECT od.origin, count(mf) as adult_memes_count FROM (
            SELECT DISTINCT 
                title, url, origin_id, adult 
            FROM 
                meme_fact
            WHERE 
                adult IN ('likely', 'possible')
        ) mf
        JOIN origin_dim od ON od.id = mf.origin_id
        GROUP BY od.origin
        ORDER BY adult_memes_count DESC;
    """)

    df_parents_with_most_spoofy_memes = hook.get_pandas_df("""
        SELECT pd.parent_link, count(mf) memes_count FROM (
            SELECT DISTINCT 
                title, url, parent_id 
            FROM 
                meme_fact
            WHERE
                spoof IN ('likely', 'possible')
        ) mf
        JOIN parent_dim pd ON pd.id = mf.parent_id
        GROUP BY pd.parent_link
        ORDER BY memes_count DESC;
    """)

    save(df_dim_counts, prefix, 'sql_analysis_dim_counts.json')
    save(df_memes_distribution_by_year, prefix, 'sql_analysis_memes_distribution_by_year.json')
    save(df_memes_distribution_by_month_name, prefix, 'sql_analysis_memes_distribution_by_month.json')
    save(df_memes_distribution_by_origin, prefix, 'sql_analysis_memes_distribution_by_origin.json')
    save(df_origins_with_most_adult_memes, prefix, 'sql_analysis_origins_wih_most_adult_memes.json')
    save(df_parents_with_most_spoofy_memes, prefix, 'sql_analysis_parents_with_most_spoofy_memes.json')


def save(df, path, name):
    df.to_json(f'{path}/{name}', orient='records')
