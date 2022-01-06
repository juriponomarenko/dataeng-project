from py2neo import Node, Relationship, Graph#, authenticate
import pandas as pd

def run_neo_ingestion():
    #data
    df=pd.read_json('/opt/airflow/dags/data/kym_vs.json')
    #database
#    graph = Graph("neo4j://localhost:7687")
    graph = Graph("bolt://neo:7687")
    #graph.delete_all()
    #fill database with data, takes time (>5min)
    #we use only meme titles and parent-child relations
    tx = graph.begin() 
    for i in range(len(df.title)):
        node_p = Node('meme',title=df.title[i])
        tx.merge(node_p, 'meme', 'title')
        chi=df.children[i]
        for j in range(len(chi)):
            node_c = Node('meme',title=chi[j])
            tx.merge(node_c, 'meme', 'title')
            relation=Relationship(node_p, "child", node_c)
            tx.merge(relation)   
    graph.commit(tx)
