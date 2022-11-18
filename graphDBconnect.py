import logging
from neo4j import (GraphDatabase,basic_auth)
from neo4j.exceptions import ServiceUnavailable  

url = "bolt://127.0.0.1:7687"
driver = GraphDatabase.driver(url, auth=("neo4j", "neo4j")) ##default authentication for testing



def store_imdb_clone(tx,name):
    tx.run("CREATE (a:Movie {name: $name})", name=name)
    
"""
def find_content(search_key):
        with driver.session() as session:
            result = session.read_transaction(find_movie_record, movie_name)
            for record in result:
                print("Found movie: {record}".format(record=record))
"""
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

"""

def create_person(tx, name):
    tx.run("CREATE (a:Person {name: $name})", name=name)


def create_friend_of(tx, name, friend):
    tx.run("MATCH (a:Person) WHERE a.name = $name "
           "CREATE (a)-[:KNOWS]->(:Person {name: $friend})",
           name=name, friend=friend)

if _name_ == "_main_":
    with driver.session() as session:
        #tx=session.begin_transaction 
        session.execute_write(create_person, "Vikram")
        session.execute_write(create_friend_of, "Vikram", "Saurabh")
        session.execute_write(create_friend_of, "Vikram", "Sanjay")

    driver.close()
"""


if __name__ == "__main__":
    #file_path="srch_data.json"
    store_json_data(param1,param2)
    with driver.session() as session:
        session.read_transaction(find_movie_record, "Inception") #similar to execute_read 
        session.execute_write(store_imdb_clone,"Test")
    driver.close()