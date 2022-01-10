from py2neo import Graph
import pandas as pd

def run_neo_analysis():
    graph = Graph("bolt://neo:7687")
    #children of meme 'hashtag'
    query='''
    MATCH (m)-[:child]->(n) 
    WHERE m.title='hashtag'
    RETURN n
    '''
    result = graph.run(query)
    df_result = result.to_data_frame()
    df_result.to_json("/opt/airflow/dags/data/neo_analysis_children_of_hashtag.json",orient='records')

    #longest path 
    query='''
    MATCH path=(m)-[:child*]->() 
    WHERE not(()-[:child]->(m))
    RETURN [t in nodes(path) | t.title],length(path)
    ORDER BY length(path) desc
    LIMIT 50
    '''
    result = graph.run(query)
    df_result = result.to_data_frame()
    df_result.to_json("/opt/airflow/dags/data/neo_analysis_longest_path50.json",orient='records')

# cypher code to see graph with longest path in neo UI in localhost
#MATCH path=(m)-[:child*]->() 
#WHERE m.title='memes'
#RETURN path
#ORDER BY length(path) desc
#LIMIT 50