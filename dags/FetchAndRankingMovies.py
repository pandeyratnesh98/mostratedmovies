import datetime as dt
import logging
import json
import os
import re
import pandas as pd
import requests

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
MOVIEREC_HOST = os.environ.get("MOVIEREC_HOST", "movierec")
MOVIEREC_SCHEMA = os.environ.get("MOVIEREC_SCHEMA", "http")
MOVIEREC_PORT = os.environ.get("MOVIEREC_PORT", "5000")

MOVIEREC_USER = os.environ["MOVIEREC_USER"]
MOVIEREC_PASSWORD = os.environ["MOVIEREC_PASSWORD"]

data_list = ["ratings", "movies", "tags"]


# pylint: disable=logging-format-interpolation

def _get_every_node(node, batch_size=100):
    session, base_url = _get_session()
    yield from _get_with_pagination(
        session=session,
        url=base_url + f"/{node}",
        batch_size=batch_size
    )


def _get_session():
    """Builds a requests Session for the Movierec API."""

    # Setup our requests session.
    session = requests.Session()
    session.auth = (MOVIEREC_USER, MOVIEREC_PASSWORD)

    # Define API base url from connection details.
    schema = MOVIEREC_SCHEMA
    host = MOVIEREC_HOST
    port = MOVIEREC_PORT

    base_url = f"{schema}://{host}:{port}"

    return session, base_url


def _get_with_pagination(session, url, batch_size=100):
    """
    Fetches records using a get request with given url/params,
    taking pagination into account.
    """

    offset = 0
    total = None
    while total is None or offset < total:
        response = session.get(
            url, params={"offset": offset, "limit": batch_size}
        )
        response.raise_for_status()
        response_json = response.json()

        yield from response_json["result"]

        offset += batch_size
        total = response_json["total"]


def extract_rel_year(row):
    try:
        return re.search(r'(?<=\().+?(?=\))', row['title']).group(0)
    except AttributeError:
        return None


with DAG(
        dag_id="fetch_movies_dag",
        description="Fetches movies from the Movierec API using the Python Operator.",
        start_date=dt.datetime(2022, 12, 28),
        end_date=dt.datetime(2023, 1, 10),
        schedule_interval="@once",
) as dag:
    def _fetch_every_node(templates_dict, batch_size=1000, **_):
        logger = logging.getLogger(__name__)
        for node in data_list:
            output_path = templates_dict["output_path"] + f"/{node}/{node}.json"
            logger.info(f"Fetching {node}...")
            data = list(_get_every_node(node=node, batch_size=batch_size))
            logger.info(f"Fetched {len(node)} {node}")

            logger.info(f"Writing {node} to {output_path}")

            # Make sure output directory exists.
            output_dir = os.path.dirname(output_path)
            os.makedirs(output_dir, exist_ok=True)

            with open(output_path, "w") as file_:
                json.dump(data, fp=file_)


    fetch_each_movieapi_node = PythonOperator(
        task_id="fetch_ratings",
        python_callable=_fetch_every_node,
        templates_dict={
            "output_path": "/data/python",
        },
    )


    def _calc_avg_ratings(templates_dict, min_ratings=1, **_):
        logger = logging.getLogger(__name__)
        input_path = templates_dict["input_path"]
        output_path = templates_dict["output_path"]

        movies = pd.read_json(input_path + "/movies/movies.json")
        logger.info("fetched movies")
        logger.info(movies.head(10).to_string())
        ratings = pd.read_json(input_path + "/ratings/ratings.json")
        logger.info("fetched ratings")
        logger.info(ratings.head(10).to_string())
        logger.info("calculating average ratings and numbers of users rated for each movie...")
        lat_ratings = (
            ratings.groupby("movieId")
            .agg(
                avg_rating=pd.NamedAgg(column="rating", aggfunc="mean"),
                num_of_ratings=pd.NamedAgg(column="userId", aggfunc="nunique"),
            )
            .sort_values(["movieId"])
        )
        latest_movies = movies.merge(lat_ratings, left_on='movieId', right_on='movieId')
        latest_movies['release_year'] = latest_movies.apply(lambda row: extract_rel_year(row), axis=1)
        # removed release year from title
        latest_movies['title'] = latest_movies['title'].str.split('(').str[0]
        # removed genres because every tags were congested at single place
        rm_genres = latest_movies.drop("genres", axis=1)
        # some release years were gibberish or values were less than 1850(first movie released in 1888) so I've
        # removed those particular rows
        rm_gibberish_year_rows = rm_genres.loc[
            rm_genres['release_year'].apply(lambda x: str(x).isdigit() and int(x) > 1850)]
        # movies sorted by ratings and years
        movies_sorted = rm_gibberish_year_rows.sort_values(['release_year', 'avg_rating'], ascending=False)
        logger.info("some latest release with highest ratings")
        logger.info(movies_sorted.head(25).to_string())

        # Make sure output directory exists.
        output_dir = os.path.dirname(output_path + "/movies/movieswithavgratings.json")
        os.makedirs(output_dir, exist_ok=True)
        movies_sorted.to_json(output_path + "/movies/movieswithavgratings.json", index=True)


    get_avg_ratings = PythonOperator(
        task_id="avg_ratings",
        python_callable=_calc_avg_ratings,
        templates_dict={
            "input_path": "/data/python",
            "output_path": "/data/python",
        },
    )


    def upload_to_s3(filename: str, key: str, bucket_name: str) -> None:
        hook = S3Hook('aws_s3_bucket')
        hook.load_file(filename=filename, key=key, bucket_name=bucket_name)


    upload_movies_data_to_s3 = PythonOperator(
        task_id='upload_to_s3',
        python_callable=upload_to_s3,
        op_kwargs={
            'filename': '/data/python/movies/movieswithavgratings.json',
            'key': 'movies.json',
            'bucket_name': 'airflow-movies-bucket'
        }
    )

    fetch_each_movieapi_node >> get_avg_ratings >> upload_movies_data_to_s3
# expression to fetch data inside the parenthesis: (?<=\().+?(?=\))
