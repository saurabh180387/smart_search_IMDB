import requests
from bs4 import BeautifulSoup
import pandas as pd
import json
from flask import (Flask, g, request,Response)
app=Flask(__name__)

base_url = 'https://www.imdb.com/search/title/?groups=top_1000&sort=user_rating,desc&count=100&start='

@app.route('/', methods=['GET'])
def get_index():
    return app.send_static_file("index.html")

def fetch_topics_page(url):
    """returns topics from url"""
    response = requests.get(url)
    # check successfull response
    if response.status_code != 200:
        raise Exception(f'Failed to load page {"topic_url"}')
    # Parse using BeautifulSoup
    doc = BeautifulSoup(response.text, 'html.parser')
    return doc


def fetch_movie_titles(doc):
        """Titles of content from movie list.."""

    selection_class = "lister-item-header"
    movie_title_tags = doc.find_all('h3', {'class': selection_class})
    movie_titles = []

    for tag in movie_title_tags:
        title = tag.find('a').text
        movie_titles.append(title)
    return movie_titles

# titles = fetch_movie_titles(doc)

def fetch_movie_url(doc):
    url_selector = "lister-item-header"
    movie_url_tags = doc.find_all('h3', {'class': url_selector})
    movie_url_tagss = []
    base_url = 'https://www.imdb.com/'
    for tag in movie_url_tags:
        movie_url_tagss.append('https://www.imdb.com/' + tag.find('a')['href'])
    return movie_url_tagss

# urls = fetch_movie_url(doc)


def fetch_movie_duration(doc):
    """how long the movie """
    selection_class = "runtime"
    movie_duration_tags = doc.find_all('span', {'class': selection_class})
    movie_duration = []

    for tag in movie_duration_tags:
        duration = tag.text[:-4]
        movie_duration.append(duration)

    return movie_duration

def fetch_movie_certification(doc):

    selection_class = "lister-item-content"
    movie_details_tags = doc.find_all('div', {'class': selection_class})
    movie_certification = []

    for detail_tag in movie_details_tags:

        certification_tag = detail_tag.find('span', {'class': 'certificate'})
        if certification_tag:
            movie_certification.append(certification_tag.text)
        else:
            movie_certification.append('NA')

    return movie_certification

def fetch_movie_year(doc):
    year_selector = "lister-item-year text-muted unbold"
    movie_year_tags = doc.find_all('span', {'class': year_selector})
    movie_year_tagss = []
    for tag in movie_year_tags:
        movie_year_tagss.append(tag.get_text().strip()[1:5])
    return movie_year_tagss

def fetch_movie_genre(doc):
    genre_selector = "genre"
    movie_genre_tags = doc.find_all('span', {'class': genre_selector})
    movie_genre_tagss = []
    for tag in movie_genre_tags:
        movie_genre_tagss.append(tag.get_text().strip())
    return movie_genre_tagss

def fetch_movie_rating(doc):
    rating_selector = "inline-block ratings-imdb-rating"
    movie_rating_tags = doc.find_all('div', {'class': rating_selector})
    movie_rating_tagss = []
    for tag in movie_rating_tags:
        movie_rating_tagss.append(tag.get_text().strip())
    return movie_rating_tagss

##@app.route('/summary')
def scrape_content(num):
    # Let's we create a dictionary to store data of all movies...taking default as 10
    movies_dict = {
        'titles': [],
        'genre': [],
        'rating': [],
        'year': [],
        'certification': [],
        'url': []
    }
    # scrap more than one page so we want urls of all pages with the help of loop we can get all urls

    for i in range(1, num * 110, 100):
        url = base_url + str(i) + '&ref_=adv_next'
        doc = fetch_topics_page(url) ##doc
        ## Invoke methods using doc found from topics url
        movies_dict['titles'] += fetch_movie_titles(doc)
        movies_dict['url'] += fetch_movie_url(doc)
        movies_dict['certification'] += fetch_movie_certification(doc)
        movies_dict['rating'] += fetch_movie_rating(doc)
        movies_dict['year'] += fetch_movie_year(doc)
        movies_dict['genre'] += fetch_movie_genre(doc)

    return pd.DataFrame(movies_dict)

def fetch_titles(doc):



"""
movies = []
def data_imdb_json(tx, variable):
    ins_query = '''CALL apoc.load.json("file:///movie_info.json")
    YIELD value
    UNWIND keys(value) AS key
    FOREACH( title in  key.titles|
    CREATE(T:title{name:title}))
    FOREACH(gnr in key.genre|
      CREATE(G:genre{name:genre}))
    MERGE (titles)-[r:RELATESTO]->(genre)
    RETURN key, apoc.meta.type(value[key]) '''
    tx.run(ins_query)


def find_movie_record(tx, name):
    c_query = '''
     MATCH (m:Movie {title:$movie})<-[:RATED]-(u:User)-[:RATED]->(rec:Movie)
     RETURN distinct rec.title AS recommendation LIMIT 20
     '''
    result = tx.run(c_query, movie=name)
    for record in result:
        movies.append(record["Movie"])
    print(result)
    return movies


"""

if __name__ == "__main__":
    imdb_content = scrape_content(num=10)
    app.run(host='127.0.0.1', port=8080, debug=True)
    """
    with driver.session() as session:
        #session.execute_write(data_imdb_json,"")
        session.execute_read(find_movie_record, "The Matrix")  # similar to execute_read
    driver.close()
    """
