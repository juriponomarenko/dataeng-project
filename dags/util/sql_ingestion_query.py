import os.path
import calendar
import pandas as pd


def generate_sql_ingestion_query():
    prefix = '/opt/airflow/dags/data'
    file_path = f'{prefix}/ingestion_q.sql'

    dates = {}
    parents = {}
    origins = {}
    children = {}
    tags = {}
    search_keywords = {}

    mapping = {
        'tags': tags,
        'search_keywords': search_keywords,
        'children': children
    }

    if os.path.exists(file_path):
        os.remove(file_path)

    with open(f'{prefix}/kym_vs.json', encoding='utf-8') as f:
        df = pd.read_json(f)

    with open(file_path, 'a+') as fw:

        for _, row in df.iterrows():

            date = None if row['year_added'] == '0001-01-01' else row['year_added']
            origin = None if row['origin'] == 'unknown' else row['origin']
            parent = None if row['parent'] == 'unknown' else row['parent']

            if date is not None and date not in dates:
                dates[date] = len(dates) + 1
                date_id = dates[date]
                year, month, day = date.split('-')
                month_name = calendar.month_name[int(month)]
                fw.write(
                    f'INSERT INTO "date_dim" ("id", "date", "month_number", "month_name", "year") VALUES ({date_id}, \'{date}\', {month}, \'{month_name}\', {year});\n')

            if parent is not None and parent not in parents:
                parents[parent] = len(parents) + 1

                parent_id = parents[parent]
                fw.write(f'INSERT INTO "parent_dim" ("id", "parent_link") VALUES ({parent_id}, \'{parent}\');\n')

            if origin is not None and origin not in origins:
                origins[origin] = len(origins) + 1

                origin_id = origins[origin]
                fw.write(f'INSERT INTO "origin_dim" ("id", "origin") VALUES ({origin_id}, \'{origin}\');\n')

            meme_base = [
                row['title'],
                row['url'],
                row['description'].replace('{{', '(').replace('}}', ')'),
                row['children_n'],
                row['tags_n'],
                len(row['search_keywords']),
                row['adult'],
                row['spoof'],
                row['medical'],
                row['racy'],
                dates[date] if date is not None else 'NULL',
                parents[parent] if parent is not None else 'NULL',
                origins[origin] if origin is not None else 'NULL',
                'NULL',
                'NULL',
                'NULL'
            ]

            for k in ['tags', 'children', 'search_keywords']:
                dataset = mapping[k]

                for e in row[k]:
                    e = e.replace("'", "''")

                    if e not in dataset:
                        dataset[e] = len(dataset) + 1

                        id = dataset[e]

                        tables = {
                            'tags': 'tag_dim',
                            'children': 'child_dim',
                            'search_keywords': 'keyword_dim'
                        }
                        cols = {
                            'tags': 'text',
                            'children': 'text',
                            'search_keywords': 'text'
                        }

                        fw.write(f'INSERT INTO "{tables[k]}" ("id", "{cols[k]}") VALUES ({id}, \'{e}\');\n')

                    meme = meme_base.copy()
                    idx = list(cols.keys()).index(k) + 1

                    meme[-idx] = dataset[e]

                    title, url, description, children_n, tags_n, keywords_n, \
                    adult, spoof, medical, racy, date_id, parent_id, origin_id, keyword_id, child_id, tag_id = meme

                    description = description.replace("'", "''")

                    fw.write(f'INSERT INTO "meme_fact"'
                             f'\n\t(title, url, description, children_count, tags_count, keywords_count, adult, spoof, '
                             f'medical, racy, date_id, parent_id, origin_id, keyword_id, child_id, tag_id)'
                             f'\nVALUES '
                             f'\n\t(\'{title}\', \'{url}\', \'{description}\', {children_n}, {tags_n}, {keywords_n},'
                             f'\'{adult}\', \'{spoof}\', \'{medical}\', \'{racy}\', {date_id}, {parent_id},'
                             f'{origin_id}, {keyword_id}, {child_id}, {tag_id});\n')

            fw.write('\n')
