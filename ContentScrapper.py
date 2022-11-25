import requests
from bs4 import BeautifulSoup
import pandas as pd
import json

import logging
from neo4j import (GraphDatabase,basic_auth)
from neo4j.exceptions import ServiceUnavailable
from flask import (Flask, g, request,Response)
#from flask_restful import Resource, reqparse,Api

url = "bolt://127.0.0.1:7687"
driver = GraphDatabase.driver(url, auth=("neo4j", "pass@123")) ##default authentication for testing

#app=Flask(__name__)
#api = Api(app) ## Not used if route mmethods are used..



class DataFrameCompilation:
    def __init__(self):
        self.base_url='https://www.imdb.com/search/title/?groups=top_1000&sort=user_rating,desc&count=100&start='
        pass

    def get_index():
        return ("Please goto proper url ")
        #return app.send_static_file("index.html")


    def fetch_topics_page(self,url):
        """returns topics from url"""   
        response=requests.get(url)
        # check successfull response
        if response.status_code != 200:
            raise Exception(f'Failed to load page {"topic_url"}')
        # Parse using BeautifulSoup
        doc = BeautifulSoup(response.text, 'html.parser')
        return doc


    def fetch_movie_titles(self,doc):
        """Titles of content from movie list.."""
        selection_class="lister-item-header"
        movie_title_tags=doc.find_all('h3',{'class':selection_class})
        movie_titles=[]
    
        for tag in movie_title_tags:
            title = tag.find('a').text
            movie_titles.append(title)
            
        return movie_titles


    def fetch_movie_url(self,doc):
        url_selector="lister-item-header"           
        movie_url_tags=doc.find_all('h3',{'class':url_selector})
        movie_url_tagss=[]
        base_url = 'https://www.imdb.com/'
        for tag in movie_url_tags:
            movie_url_tagss.append('https://www.imdb.com/' + tag.find('a')['href'])
        return movie_url_tagss
    
    #urls = fetch_movie_url(doc)

    def fetch_movie_duration(self,doc):
        """how long the movie """
        selection_class="runtime"
        movie_duration_tags=doc.find_all('span',{'class':selection_class})
        movie_duration=[]
    
        for tag in movie_duration_tags:
            duration = tag.text[:-4]
            movie_duration.append(duration)
           
        return movie_duration
    
    
    def fetch_movie_certification(self,doc):
        
        selection_class="lister-item-content"
        movie_details_tags = doc.find_all('div',{'class':selection_class})
        movie_certification=[]
        
    
        for detail_tag in movie_details_tags:
            
            certification_tag = detail_tag.find('span',{'class':'certificate'})
            if certification_tag:
                movie_certification.append(certification_tag.text)
            else:
                movie_certification.append('NA')                                                           
            
        return movie_certification
    
    
    def fetch_movie_year(self,doc):
        year_selector = "lister-item-year text-muted unbold"           
        movie_year_tags=doc.find_all('span',{'class':year_selector})
        movie_year_tagss=[]
        for tag in movie_year_tags:
            movie_year_tagss.append(tag.get_text().strip()[1:5])
        return movie_year_tagss
    
    
    def fetch_movie_genre(self,doc):
        genre_selector="genre"            
        movie_genre_tags=doc.find_all('span',{'class':genre_selector})
        movie_genre_tagss=[]
        for tag in movie_genre_tags:
            movie_genre_tagss.append(tag.get_text().strip())
        return movie_genre_tagss
    
    
    def fetch_movie_rating(self,doc):
        rating_selector="inline-block ratings-imdb-rating"            
        movie_rating_tags=doc.find_all('div',{'class':rating_selector})
        movie_rating_tagss=[]
        for tag in movie_rating_tags:
            movie_rating_tagss.append(tag.get_text().strip())
        return movie_rating_tagss

    def scrape_content(self,num):
        # Let's we create a dictionary to store data of all movies...taking default as 10
        movies_dict={
            'titles':[],
            'genre':[],
            'rating':[],
            'year':[],
            'certification':[],
            'url':[]
        }
  # scrap more than one page so we want urls of all pages with the help of loop we can get all urls

        for i in range(1,num*110,100):
            url=self.base_url+str(i)+'&ref_=adv_next'
            doc = self.fetch_topics_page(url)
            ## Invoke methods using doc found from topics url    
            movies_dict['titles'] += self.fetch_movie_titles(doc)
            movies_dict['url'] += self.fetch_movie_url(doc)
            movies_dict['certification'] += self.fetch_movie_certification(doc)
            movies_dict['rating'] += self.fetch_movie_rating(doc)
            #movies_dict['duration'] += self.fetch_movie_duration(doc)
            movies_dict['year'] += self.fetch_movie_year(doc)
            movies_dict['genre'] += self.fetch_movie_genre(doc)   
            
        return pd.DataFrame(movies_dict)    

movies = []

def insert_json_data(tx,key):
    ins_query = '''CALL apoc.load.json("file:///movie_info.json")
        YIELD value
        UNWIND keys(value) AS key
        RETURN key, apoc.meta.type(value[key]) '''
    tx.run(ins_query)


def data_imdb_json(tx):
    insert_query='''
    CREATE (TheMatrix:Movie {title:'The Matrix', released:1999, tagline:'Welcome to the Real World'})
    CREATE (Keanu:Person {name:'Keanu Reeves', born:1964})
    CREATE (Carrie:Person {name:'Carrie-Anne Moss', born:1967})
    CREATE (Laurence:Person {name:'Laurence Fishburne', born:1961})
    CREATE (Hugo:Person {name:'Hugo Weaving', born:1960})
    CREATE (LillyW:Person {name:'Lilly Wachowski', born:1967})
    CREATE (LanaW:Person {name:'Lana Wachowski', born:1965})
    CREATE (JoelS:Person {name:'Joel Silver', born:1952})
    CREATE
    (Keanu)-[:ACTED_IN {roles:['Neo']}]->(TheMatrix),
    (Carrie)-[:ACTED_IN {roles:['Trinity']}]->(TheMatrix),
    (Laurence)-[:ACTED_IN {roles:['Morpheus']}]->(TheMatrix),
    (Hugo)-[:ACTED_IN {roles:['Agent Smith']}]->(TheMatrix),
    (LillyW)-[:DIRECTED]->(TheMatrix),
    (LanaW)-[:DIRECTED]->(TheMatrix),
    (JoelS)-[:PRODUCED]->(TheMatrix)
    
    CREATE (Emil:Person {name:"Emil Eifrem", born:1978})
    CREATE (Emil)-[:ACTED_IN {roles:["Emil"]}]->(TheMatrix)
    
    CREATE (TheMatrixReloaded:Movie {title:'The Matrix Reloaded', released:2003, tagline:'Free your mind'})
    CREATE
    (Keanu)-[:ACTED_IN {roles:['Neo']}]->(TheMatrixReloaded),
    (Carrie)-[:ACTED_IN {roles:['Trinity']}]->(TheMatrixReloaded),
    (Laurence)-[:ACTED_IN {roles:['Morpheus']}]->(TheMatrixReloaded),
    (Hugo)-[:ACTED_IN {roles:['Agent Smith']}]->(TheMatrixReloaded),
    (LillyW)-[:DIRECTED]->(TheMatrixReloaded),
    (LanaW)-[:DIRECTED]->(TheMatrixReloaded),
    (JoelS)-[:PRODUCED]->(TheMatrixReloaded)
    
    CREATE (TheMatrixRevolutions:Movie {title:'The Matrix Revolutions', released:2003, tagline:'Everything that has a beginning has an end'})
    CREATE
    (Keanu)-[:ACTED_IN {roles:['Neo']}]->(TheMatrixRevolutions),
    (Carrie)-[:ACTED_IN {roles:['Trinity']}]->(TheMatrixRevolutions),
    (Laurence)-[:ACTED_IN {roles:['Morpheus']}]->(TheMatrixRevolutions),
    (Hugo)-[:ACTED_IN {roles:['Agent Smith']}]->(TheMatrixRevolutions),
    (LillyW)-[:DIRECTED]->(TheMatrixRevolutions),
    (LanaW)-[:DIRECTED]->(TheMatrixRevolutions),
    (JoelS)-[:PRODUCED]->(TheMatrixRevolutions)
    
    CREATE (TheDevilsAdvocate:Movie {title:"The Devil's Advocate", released:1997, tagline:'Evil has its winning ways'})
    CREATE (Charlize:Person {name:'Charlize Theron', born:1975})
    CREATE (Al:Person {name:'Al Pacino', born:1940})
    CREATE (Taylor:Person {name:'Taylor Hackford', born:1944})
    CREATE
    (Keanu)-[:ACTED_IN {roles:['Kevin Lomax']}]->(TheDevilsAdvocate),
    (Charlize)-[:ACTED_IN {roles:['Mary Ann Lomax']}]->(TheDevilsAdvocate),
    (Al)-[:ACTED_IN {roles:['John Milton']}]->(TheDevilsAdvocate),
    (Taylor)-[:DIRECTED]->(TheDevilsAdvocate)
    
    CREATE (AFewGoodMen:Movie {title:"A Few Good Men", released:1992, tagline:"In the heart of the nation's capital, in a courthouse of the U.S. government, one man will stop at nothing to keep his honor, and one will stop at nothing to find the truth."})
    CREATE (TomC:Person {name:'Tom Cruise', born:1962})
    CREATE (JackN:Person {name:'Jack Nicholson', born:1937})
    CREATE (DemiM:Person {name:'Demi Moore', born:1962})
    CREATE (KevinB:Person {name:'Kevin Bacon', born:1958})
    CREATE (KieferS:Person {name:'Kiefer Sutherland', born:1966})
    CREATE (NoahW:Person {name:'Noah Wyle', born:1971})
    CREATE (CubaG:Person {name:'Cuba Gooding Jr.', born:1968})
    CREATE (KevinP:Person {name:'Kevin Pollak', born:1957})
    CREATE (JTW:Person {name:'J.T. Walsh', born:1943})
    CREATE (JamesM:Person {name:'James Marshall', born:1967})
    CREATE (ChristopherG:Person {name:'Christopher Guest', born:1948})
    CREATE (RobR:Person {name:'Rob Reiner', born:1947})
    CREATE (AaronS:Person {name:'Aaron Sorkin', born:1961})
    CREATE
    (TomC)-[:ACTED_IN {roles:['Lt. Daniel Kaffee']}]->(AFewGoodMen),
    (JackN)-[:ACTED_IN {roles:['Col. Nathan R. Jessup']}]->(AFewGoodMen),
    (DemiM)-[:ACTED_IN {roles:['Lt. Cdr. JoAnne Galloway']}]->(AFewGoodMen),
    (KevinB)-[:ACTED_IN {roles:['Capt. Jack Ross']}]->(AFewGoodMen),
    (KieferS)-[:ACTED_IN {roles:['Lt. Jonathan Kendrick']}]->(AFewGoodMen),
    (NoahW)-[:ACTED_IN {roles:['Cpl. Jeffrey Barnes']}]->(AFewGoodMen),
    (CubaG)-[:ACTED_IN {roles:['Cpl. Carl Hammaker']}]->(AFewGoodMen),
    (KevinP)-[:ACTED_IN {roles:['Lt. Sam Weinberg']}]->(AFewGoodMen),
    (JTW)-[:ACTED_IN {roles:['Lt. Col. Matthew Andrew Markinson']}]->(AFewGoodMen),
    (JamesM)-[:ACTED_IN {roles:['Pfc. Louden Downey']}]->(AFewGoodMen),
    (ChristopherG)-[:ACTED_IN {roles:['Dr. Stone']}]->(AFewGoodMen),
    (AaronS)-[:ACTED_IN {roles:['Man in Bar']}]->(AFewGoodMen),
    (RobR)-[:DIRECTED]->(AFewGoodMen),
    (AaronS)-[:WROTE]->(AFewGoodMen)
    
    CREATE (TopGun:Movie {title:"Top Gun", released:1986, tagline:'I feel the need, the need for speed.'})
    CREATE (KellyM:Person {name:'Kelly McGillis', born:1957})
    CREATE (ValK:Person {name:'Val Kilmer', born:1959})
    CREATE (AnthonyE:Person {name:'Anthony Edwards', born:1962})
    CREATE (TomS:Person {name:'Tom Skerritt', born:1933})
    CREATE (MegR:Person {name:'Meg Ryan', born:1961})
    CREATE (TonyS:Person {name:'Tony Scott', born:1944})
    CREATE (JimC:Person {name:'Jim Cash', born:1941})
    CREATE
    (TomC)-[:ACTED_IN {roles:['Maverick']}]->(TopGun),
    (KellyM)-[:ACTED_IN {roles:['Charlie']}]->(TopGun),
    (ValK)-[:ACTED_IN {roles:['Iceman']}]->(TopGun),
    (AnthonyE)-[:ACTED_IN {roles:['Goose']}]->(TopGun),
    (TomS)-[:ACTED_IN {roles:['Viper']}]->(TopGun),
    (MegR)-[:ACTED_IN {roles:['Carole']}]->(TopGun),
    (TonyS)-[:DIRECTED]->(TopGun),
    (JimC)-[:WROTE]->(TopGun)
    
    CREATE (JerryMaguire:Movie {title:'Jerry Maguire', released:2000, tagline:'The rest of his life begins now.'})
    CREATE (ReneeZ:Person {name:'Renee Zellweger', born:1969})
    CREATE (KellyP:Person {name:'Kelly Preston', born:1962})
    CREATE (JerryO:Person {name:"Jerry O'Connell", born:1974})
    CREATE (JayM:Person {name:'Jay Mohr', born:1970})
    CREATE (BonnieH:Person {name:'Bonnie Hunt', born:1961})
    CREATE (ReginaK:Person {name:'Regina King', born:1971})
    CREATE (JonathanL:Person {name:'Jonathan Lipnicki', born:1996})
    CREATE (CameronC:Person {name:'Cameron Crowe', born:1957})
    CREATE
    (TomC)-[:ACTED_IN {roles:['Jerry Maguire']}]->(JerryMaguire),
    (CubaG)-[:ACTED_IN {roles:['Rod Tidwell']}]->(JerryMaguire),
    (ReneeZ)-[:ACTED_IN {roles:['Dorothy Boyd']}]->(JerryMaguire),
    (KellyP)-[:ACTED_IN {roles:['Avery Bishop']}]->(JerryMaguire),
    (JerryO)-[:ACTED_IN {roles:['Frank Cushman']}]->(JerryMaguire),
    (JayM)-[:ACTED_IN {roles:['Bob Sugar']}]->(JerryMaguire),
    (BonnieH)-[:ACTED_IN {roles:['Laurel Boyd']}]->(JerryMaguire),
    (ReginaK)-[:ACTED_IN {roles:['Marcee Tidwell']}]->(JerryMaguire),
    (JonathanL)-[:ACTED_IN {roles:['Ray Boyd']}]->(JerryMaguire),
    (CameronC)-[:DIRECTED]->(JerryMaguire),
    (CameronC)-[:PRODUCED]->(JerryMaguire),
    '''
    tx.run(insert_query)


def find_movie_record(tx, title):
    c_query='''
    MATCH (movie:Movie {title:$title})<-[:ACTED_IN]-(actor)-[:ACTED_IN]->(rec:Movie)
    RETURN distinct rec.title,movie as title LIMIT 100
    '''
    #result1 = tx.run("MATCH (movie:Movie {title:$title}) "\
    #        "OPTIONAL MATCH (movie)<-[b]-(genre:Genre) "\
    #        "RETURN movie"\
    #        "LIMIT 20",{"title": title})
    result=tx.run(c_query,{"title": title})
    print(list(result))

#class RenderFlaskTemp:



if __name__ == "__main__":
    '''
    DF_Obj=DataFrameCompilation()
    imdb_content = DF_Obj.scrape_content(num=10)
    #imdb_content.to_csv('movies.csv',index=None)
    json_dataframe = imdb_content.to_json()
    print(json_dataframe)
    with open('E:\\neo4j\\import\\movie_info.json','w') as fileObj:
        json.dump(json_dataframe,fileObj,indent=4)
    '''

    with driver.session() as session:
        #session.execute_write(data_imdb_json)
        session.execute_read(find_movie_record, "The Matrix")  # similar to execute_read
    driver.close()
