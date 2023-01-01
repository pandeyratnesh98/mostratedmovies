import os
import time
import logging

import pandas as pd

from flask import Flask, jsonify, request
from flask_httpauth import HTTPBasicAuth
from werkzeug.security import generate_password_hash, check_password_hash

DEFAULT_ITEMS_PER_PAGE = 100
data_list = ["genome-scores", "tags", "links", "ratings", "genome-tags", "genome-scores", "movies"]


def _read_data_from_path(file_path):
    data = pd.read_csv(file_path)
    # Sort by ts, user, movie for convenience.
    if file_path == "/ratings.csv":
        return data.sort_values(by=["timestamp", "userId", "movieId"])

    return data


app = Flask(__name__)
for data_path in data_list:
    try:
        app.config[data_path] = _read_data_from_path(f"/{data_path}.csv")
    except FileNotFoundError:
        logging.info(f"File not found")


auth = HTTPBasicAuth()
users = {os.environ["API_USER"]: generate_password_hash(os.environ["API_PASSWORD"])}


@auth.verify_password
def verify_password(username, password):
    if username in users:
        return check_password_hash(users.get(username), password)
    return False


@app.route("/<dataname>")
@auth.login_required
def ratings(dataname=None):
    """
    Returns ratings from the movielens dataset.
    Parameters
    ----------
    start_date : str
        Start date to query from (inclusive).
    end_date : str
        End date to query upto (exclusive).
    offset : int
        Offset to start returning data from (used for pagination).
    limit : int
        Maximum number of records to return (used for pagination).
        :param dataname: title to fetch the data from e.g. ratings & movies
    """
    offset = int(request.args.get("offset", 0))
    limit = int(request.args.get("limit", DEFAULT_ITEMS_PER_PAGE))
    if dataname is not None and dataname in data_list and dataname != "ratings":
        data_df = app.config.get(dataname)
        subset = data_df.iloc[offset: offset + limit]

        return jsonify(
            {
                "result": subset.to_dict(orient="records"),
                "offset": offset,
                "limit": limit,
                "total": data_df.shape[0],
            })

    elif dataname == "ratings":
        start_date_ts = _date_to_timestamp(request.args.get("start_date", None))
        end_date_ts = _date_to_timestamp(request.args.get("end_date", None))
        ratings_df = app.config.get("ratings")

        if start_date_ts:
            ratings_df = ratings_df.loc[ratings_df["timestamp"] >= start_date_ts]

        if end_date_ts:
            ratings_df = ratings_df.loc[ratings_df["timestamp"] < end_date_ts]

        subset = ratings_df.iloc[offset: offset + limit]

        return jsonify(
            {
                "result": subset.to_dict(orient="records"),
                "offset": offset,
                "limit": limit,
                "total": ratings_df.shape[0],
            })
    else:
        return """Welcome to MovieRec APi
         pages available are:
         1.ratings
         2.genome-scores
         3.tags 
         4.links 
         5.genome-tags 
         6.genome-scores 
         7.movies    
         """


def _date_to_timestamp(date_str):
    if date_str is None:
        return None
    return int(time.mktime(time.strptime(date_str, "%Y-%m-%d")))


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
