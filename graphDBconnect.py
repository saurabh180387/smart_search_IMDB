import logging
from neo4j import GraphDatabase
from neo4j.exceptions import ServiceUnavailable  

url = "neo4j://localhost:7687"
driver = GraphDatabase.driver(url, auth=("", "")) ##removed authentication for testing

def store_json_data(self,message):
    query="WITH 'file:///srch_data.json' as uri \ 
       CAL apoc.load.json(url) YIELD value  \
       UNWND  value.CVE_Items as data \
       RETRN data limit 5"
    with driver.session() as session:
        session.write_transaction(query,file_name)

def find_content(self, search_key):
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
    file_name="srch_data.json"
    store_json_data(message='')
    find_content(search_key='Inception')
    driver.close()