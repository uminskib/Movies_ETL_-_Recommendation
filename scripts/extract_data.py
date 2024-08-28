import requests

from google.cloud import storage
import json
from datetime import datetime


def extract_movie_data(bucket_name):

    with open("./config/tmdb_api_key.json") as f:
        api_key = json.load(f)["api_key"]  # YOUR_TMDB_API_KEY
    merged_movie_data = []
    # Get trending movies from themoviedb api
    for i in range(1, 251):  # max if 501, i.e. around 10000 movies
        url = "https://api.themoviedb.org/3/trending/movie/week?api_key={0}&page={1}".format(
            api_key, i
        )
        response = requests.get(url)
        data = response.json()["results"]
        merged_movie_data.append(data)
    merged_movie_data_one_list = [x for xs in merged_movie_data for x in xs]

    # Get movie genres from api
    url = "https://api.themoviedb.org/3/genre/movie/list?api_key={0}".format(api_key)
    response = requests.get(url)
    genre_data = response.json()["genres"]

    # Save to GCS
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    blob_trending_movies = bucket.blob(
        f"raw/trending_movies_{datetime.now().strftime('%Y%m%d')}.json"
    )
    blob_trending_movies.upload_from_string(json.dumps(merged_movie_data_one_list))
    blob_genre = bucket.blob(
        f"raw/movie_genres_{datetime.now().strftime('%Y%m%d')}.json"
    )
    blob_genre.upload_from_string(json.dumps(genre_data))
