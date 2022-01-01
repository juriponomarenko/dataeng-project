import pandas as pd
import ijson


def clean():

    file_name = "/opt/airflow/dags/data/kym_vision.json"

    # parse large json by ijson and collect title, safeSearchAnnotation and bestGuessLabels fields
    data = []
    title = ''
    A = ''
    S = ''
    M = ''
    V = ''
    R = ''
    L = ''
    with open(file_name, encoding='utf-8') as file:
        parser = ijson.parse(file)
        for prefix, event, value in parser:
            if (prefix == '') & (event == 'map_key'):
                title = value
            if prefix == title + '.safeSearchAnnotation.adult':
                A = value.replace('_', ' ').lower()
            if prefix == title + '.safeSearchAnnotation.spoof':
                S = value.replace('_', ' ').lower()
            if prefix == title + '.safeSearchAnnotation.medical':
                M = value.replace('_', ' ').lower()
            if prefix == title + '.safeSearchAnnotation.violence':
                V = value.replace('_', ' ').lower()
            if prefix == title + '.safeSearchAnnotation.racy':
                R = value.replace('_', ' ').lower()
            if prefix == title + '.webDetection.bestGuessLabels.item.label':
                L = value.replace('\'', '').lower()
            if (prefix == title) & (event == 'end_map'):
                row = {'title': title, 'adult': A, 'spoof': S, 'medical': M, 'violence': V, 'racy': R, 'label': L}
                data.append(row)
                row = {}

                # pack into DataFrame
    df = pd.DataFrame(data)

    # title as url ending
    df['title'] = df.apply(lambda row: row['title'].split('memes/')[1].replace('-', ' ').lower(), axis=1)

    # remove duplicates
    df = df.drop_duplicates(["title"])

    # finalising
    df_final = df[['title', 'adult', 'spoof', 'medical', 'violence', 'racy', 'label']]
    df_final.to_json("/opt/airflow/dags/data/kym_vision_cleaned.json", orient='records')