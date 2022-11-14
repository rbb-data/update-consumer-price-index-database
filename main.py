import os
import datetime
import logging

import numpy as np
import pandas as pd
import requests
from dateutil.relativedelta import relativedelta

from google.cloud import secretmanager

client = secretmanager.SecretManagerServiceClient()

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%d-%m-%y %H:%M:%S",
    level=logging.INFO,
)

root = os.path.dirname(os.path.abspath(__file__))
IDS_FILENAME = os.path.join(root, "data", "warenkorb_ids.txt")

if "FUNCTION_ENV" in os.environ and os.environ["FUNCTION_ENV"] == "development":
    GENESIS_FILENAME = os.path.join(root, "tmp", "genesis_tmp.csv")
else:
    GENESIS_FILENAME = os.path.join("/tmp/genesis_tmp.csv")

os.makedirs(os.path.dirname(GENESIS_FILENAME), exist_ok=True)


def download(url, params, filename):
    logging.info("GET " + url)

    try:
        response = requests.get(url, params=params)
    except Exception as e:
        return {"ok": False, "code": "request_failed", "message": str(e)}

    if not response.ok:
        return {"ok": False, "code": "request_failed", "message": response.status_code}

    # in case the request is invalid
    try:
        json_response = response.json()
        if json_response["Status"]["Code"] != 0:
            return {
                "ok": False,
                "code": "request_invalid",
                "message": json_response["Status"]["Content"],
            }
    except:
        pass

    with open(filename, "wb") as f:
        f.write(response.content)

    return {"ok": True}


def parse_raw_data(filename):
    COLUMNS = {
        "3_Auspraegung_Code": "id",
        "3_Auspraegung_Label": "name",
        "Zeit": "year",
        "2_Auspraegung_Label": "month",
        "PREIS1__Verbraucherpreisindex__2015=100": "value",
    }

    # read data and rename relevant columns
    df = pd.read_csv(filename, sep=";")
    df = df[COLUMNS.keys()].rename(columns=COLUMNS)

    # transform months into numerical format, e.g. Februar -> 2
    MONTH_NUMBER = {
        "Januar": 1,
        "Februar": 2,
        "März": 3,
        "April": 4,
        "Mai": 5,
        "Juni": 6,
        "Juli": 7,
        "August": 8,
        "September": 9,
        "Oktober": 10,
        "November": 11,
        "Dezember": 12,
    }
    df["month"] = df.month.apply(lambda month: MONTH_NUMBER[month])

    # drop rows without data
    df = df[df.value != "..."]

    # parse values as float
    df["value"] = df.value.str.replace(",", ".")
    df["value"] = pd.to_numeric(df.value, errors="coerce")

    # re-order
    df = df[["id", "name", "year", "month", "value"]]

    return df


def access_secret(name):
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8")


def update_database(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic."""
    # read warenkorb items to be requested from file
    with open(IDS_FILENAME) as f:
        item_ids = [_id.strip() for _id in f.readlines()]

    try:
        response = requests.get(
            os.environ["API_URL"],
            params={
                "table": "consumer-price-index",
                "mode": "most-recent-date",
                "id": "CC13-0111101100",
            },
        )
    except Exception as e:
        logging.error("GET request failed: " + str(e))
        return

    if not response.ok:
        logging.error("GET request failed: " + str(response.url))
        return

    most_recent_date = response.json()
    curr_month = datetime.date(most_recent_date["year"], most_recent_date["month"], 1)
    next_month = curr_month + relativedelta(months=+1)

    genesis_query_params = {
        "username": os.environ["GENESIS_USERNAME"],
        "password": access_secret(os.environ["GENESIS_PASSWORD"]),
        "language": "de",
        "name": "61111-0006",
        "area": "all",
        "startyear": next_month.year,
        "endyear": next_month.year,
        "classifyingvariable1": "CC13Z1",
        "classifyingkey1": ",".join(item_ids),
        "format": "ffcsv",
    }

    # download data from genesis as csv file
    download_response = download(
        os.environ["GENESIS_API_URL"],
        params=genesis_query_params,
        filename=GENESIS_FILENAME,
    )
    if not download_response["ok"]:
        logging.error("GET request failed: " + str(download_response["message"]))
        return

    # parse csv file and get updated data
    df = parse_raw_data(GENESIS_FILENAME)
    df_updated = df[df.month >= next_month.month]

    # clean up
    if os.path.exists(GENESIS_FILENAME):
        os.remove(GENESIS_FILENAME)

    if len(df_updated) == 0:
        logging.info("No new data available")
        return

    updated_records = df_updated.replace({np.nan: None}).to_dict("records")

    logging.info(f"POST {os.environ['API_URL']} ({len(updated_records)} items)")
    post_response = requests.post(
        os.environ["API_URL"],
        params={"table": "consumer-price-index"},
        json=updated_records,
        headers={"Authorization": "Bearer " + access_secret(os.environ["API_SECRET"])},
    )

    if not post_response.ok:
        logging.error(f"POST request failed: {post_response.status_code}")
        return
