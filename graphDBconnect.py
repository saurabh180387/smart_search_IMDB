import logging
from neo4j import (GraphDatabase,basic_auth)
from neo4j.exceptions import ServiceUnavailable  
#import  ContentScrapper

url = "bolt://127.0.0.1:7687"
driver = GraphDatabase.driver(url, auth=("neo4j", "pass@123")) ##default authentication for testing


def data_imdb_json(tx,var_name):
    ins_query='''WITH apoc.convert.fromJsonMap($jsonString) as value
   UNWIND keys(value) AS key
   RETURN key, apoc.meta.type(value[key]);'''
    tx.run(ins_query, jsonString=var_name)

def find_movie_record(tx, name):
    movies = []
    c_query='''
     MATCH (m:Movie {title:$movie})<-[:RATED]-(u:User)-[:RATED]->(rec:Movie)
     RETURN distinct rec.title AS recommendation LIMIT 20
     '''
    result = tx.run(c_query, movie=name)
    for record in result:
        movies.append(record["Movie"])
    return movies

if __name__ == "__main__":
    global json_dataframe
    print(json_dataframe)
    with driver.session() as session:
        session.execute_write(data_imdb_json,json_dataframe)
        session.execute_read(find_movie_record, "Inception") #similar to execute_read

    driver.close()