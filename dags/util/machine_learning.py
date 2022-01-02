import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor

def run_ml_analysis():
    # ### Enrich "kym_vs.csv" file
    df = pd.read_json('/opt/airflow/dags/data/kym_vs.json')
    df0 = df[["description_n", "tags_n", "children_n", "siblings_n", "DBPedia_resources_n"]]
    df0_1 = df0[df0.DBPedia_resources_n != 0]
    df0_0 = df0[df0.DBPedia_resources_n == 0]
    # Fit data
    X_all, y_all = df0_1.loc[:, df0_1.columns != "DBPedia_resources_n"], df0_1.DBPedia_resources_n
    rf = RandomForestRegressor(n_estimators=500, random_state = 11, min_samples_split = 2)
    rf.fit(X_all, y_all)
    # Evaluation
    #print("MAE:", np.mean(np.abs(rf.predict(X_all) - y_all)))
    resources_n0 = rf.predict(df0_0.loc[:, df0_0.columns != "DBPedia_resources_n"])
    df.loc[df.DBPedia_resources_n == 0, "DBPedia_resources_n"] = resources_n0.round()
    #Save enriched data
    df.to_json("/opt/airflow/dags/data/ml_analysis_result_kym_vs2.json")

