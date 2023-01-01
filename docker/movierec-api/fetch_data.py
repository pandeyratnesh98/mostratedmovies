import logging
from pathlib import Path
import tempfile
from urllib.request import urlretrieve
import zipfile

import click
import pandas as pd
import re

logging.basicConfig(
    format="[%(asctime)-15s] %(levelname)s - %(message)s", level=logging.INFO
)


@click.command()
@click.option("--start_date", default="2019-01-01", type=click.DateTime())
@click.option("--end_date", default="2020-01-01", type=click.DateTime())
def main(start_date, end_date):
    """Script for fetching movierec ratings within a given date range."""

    logging.info(f"Fetching all data...")

    """Fetches ratings from the given URL."""

    url = "http://files.grouplens.org/datasets/movielens/ml-25m.zip"

    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_path = Path(tmp_dir, "download.zip")
        logging.info(f"Downloading zip file from {url}")
        urlretrieve(url, tmp_path)

        with zipfile.ZipFile(tmp_path) as zip_:
            logging.info(f"Downloaded zip file with contents: {zip_.namelist()}")
            for file_name in zip_.namelist():
                if ".csv" in file_name:
                    with zip_.open(file_name) as file_:
                        data = pd.read_csv(file_)
                        if "ml-25m/ratings.csv" == file_name:
                            logging.info(f"Reading {file_name} from zip file")
                            logging.info(f"Filtering for dates {start_date} - {end_date}...")
                            ts_parsed = pd.to_datetime(data["timestamp"], unit="s")
                            ratings = data.loc[(ts_parsed >= start_date) & (ts_parsed < end_date)]
                            logging.info(f"Writing ratings to 'ratings.csv'...")
                            ratings.to_csv(f"/ratings.csv", index=False)
                        else:
                            file = re.search(r"(?<=/).*", file_name)
                            logging.info(f"Writing {file.group(0)} to '{file.group(0)}'...")
                            data.to_csv(f"/{file.group(0)}", index=False)


if __name__ == "__main__":
    main()
