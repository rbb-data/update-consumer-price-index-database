#!/usr/bin/env python3

import datetime
import locale
import logging
import os
import sys

import numpy as np
import pandas as pd
import requests
from dateutil.relativedelta import relativedelta

import secret_config as CONFIG

locale.setlocale(locale.LC_TIME, "de_DE.UTF-8")
logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%d-%m-%y %H:%M:%S",
    level=logging.INFO,
)

IDS_FILENAME = "warenkorb_ids.txt"

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
IDS_PATH = os.path.join(SCRIPT_DIR, IDS_FILENAME)

GENESIS_API_URL = "https://www-genesis.destatis.de/genesisWS/rest/2020/data/tablefile"
API_URL = "https://europe-west3-rbb-data-inflation.cloudfunctions.net/consumer-price-index-api"


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
    df["month"] = df.month.apply(
        lambda month: datetime.datetime.strptime(month, "%B").date().month
    )

    # drop rows without data
    df = df[df.value != "..."]

    # parse values as float
    df["value"] = df.value.str.replace(",", ".")
    df["value"] = pd.to_numeric(df.value, errors="coerce")

    # re-order
    df = df[["id", "name", "year", "month", "value"]]

    return df


def main():
    # read warenkorb items to be requested from file
    with open(IDS_PATH) as f:
        item_ids = [_id.strip() for _id in f.readlines()]

    try:
        response = requests.get(
            API_URL,
            params={
                "table": "consumer-price-index",
                "mode": "most-recent-date",
                "id": "CC13-0111101100",
            },
        )
    except Exception as e:
        logging.error("GET request failed: " + str(e))
        sys.exit(1)

    if not response.ok:
        logging.error("GET request failed: " + str(response.url))
        sys.exit(1)

    most_recent_date = response.json()
    curr_month = datetime.date(most_recent_date["year"], most_recent_date["month"], 1)
    next_month = curr_month + relativedelta(months=+1)

    genesis_query_params = {
        "username": CONFIG.GENESIS_USERNAME,
        "password": CONFIG.GENESIS_PASSWORD,
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
    filename = os.path.join(SCRIPT_DIR, "tmp.csv")
    download_response = download(
        GENESIS_API_URL, params=genesis_query_params, filename=filename
    )
    if not download_response["ok"]:
        logging.error("GET request failed: " + str(download_response["message"]))
        sys.exit(1)

    # parse csv file and get updated data
    df = parse_raw_data(filename)
    df_updated = df[df.month >= next_month.month]

    if len(df_updated) == 0:
        logging.info("No new data available")
        sys.exit(0)

    updated_records = df_updated.replace({np.nan: None}).to_dict("records")

    logging.info(f"POST {API_URL} ({len(updated_records)} items)")
    post_response = requests.post(
        API_URL,
        params={"table": "consumer-price-index"},
        json=updated_records,
        headers={"Authorization": "Bearer " + CONFIG.API_TOKEN},
    )

    if not post_response.ok:
        logging.error(f"POST request failed: {post_response.status_code}")
        sys.exit(1)


if __name__ == "__main__":
    main()
