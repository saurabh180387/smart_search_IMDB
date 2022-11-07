import logging
from neo4j import (GraphDatabase,basic_auth)
from neo4j.exceptions import ServiceUnavailable  

url = "neo4j://localhost:7687"
driver = GraphDatabase.driver(url, auth=("", "")) ##removed authentication for testing

def store_json_data(file_path):
    query="WITH 'file:///$file_path' as uri"
       "CAL apoc.load.json(url) YIELD value"
       "UNWND  value.CVE_Items as data"
       "RETRN data limit 5"
    with driver.session() as session:
        session.write_transaction(query,file_path)

def find_content(search_key):
        with driver.session() as session:
            result = session.read_transaction(find_movie_record, movie_name)
            for record in result:
                print("Found movie: {record}".format(record=record))

def find_movie_record(tx, name):
    movies = []
    result = tx.run("MATCH (a:Movie) "
                    "WHERE a.name = $name "
                    "RETURN f.name AS movies", name=name)
    for record in result:
        movies.append(record["Movie"])
    return movies




if __name__ == "__main__":
    file_path="srch_data.json"
    store_json_data(file_path,message='')
    find_content(search_key='Inception')
    driver.close()