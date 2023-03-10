import json
import os
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional

import boto3
import requests as re
from loggers import get_logger

AWS_ACCESS_KEY_ID = os.environ["AWS_ACCESS_KEY_ID"]
AWS_SECRET_ACCESS_KEY = os.environ["AWS_SECRET_ACCESS_KEY"]
API_KEY_NABLA = os.environ["API_KEY_NABLA"]

BUCKET_NAME = "nablalog"

headers = {
    "Accept": "application/json",
    "Authorization": f"Bearer {API_KEY_NABLA}",
    "Content-Type": "application/json",
}

log = get_logger(Path(__file__).stem)


def get_client(
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
):
    return boto3.client(
        "s3",
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
    )


def save_data(s3, bucket_name: str, data: Dict[str, str], entity: Optional[str] = None):
    for item in data:
        entity_name = entity or item["type"]
        s3.put_object(
            Bucket=bucket_name,
            Key=f"{entity_name}/{item['id']}.json",
            Body=json.dumps(item),
        )


def get_nabla_data(
    url: str,
    bucket_name: str,
    entity: Optional[str] = None,
    req_headers: Dict[str, str] = headers,
    url_sep: str = "?",
) -> list:
    req = re.get(url, headers=req_headers).json()
    data = [req["data"]]

    s3 = get_client()

    if len(data[0]) > 0:
        save_data(s3, bucket_name, entity, data[0])
        while req["has_more"]:
            cursor = req["next_cursor"]
            req = re.get(url + url_sep + "cursor=" + str(cursor), headers=req_headers).json()
            data_cursor = req["data"]
            save_data(s3, bucket_name, entity, data_cursor)
            data.append(data_cursor)

    return data


def parse_iso_date(date_value: str):
    return datetime.strptime(date_value.split(".")[0], "%Y-%m-%dT%H:%M:%S")


def get_incremental_nabla_data(
    url: str,
    bucket_name: str,
    req_headers: Dict[str, str] = headers,
):
    s3 = get_client()
    last_date = s3.get_object(Bucket=bucket_name, Key="max_date.txt")["Body"].read().decode("utf-8")
    url += last_date

    nabla_data = get_nabla_data(
        url=url,
        bucket_name=bucket_name,
        req_headers=req_headers,
        url_sep="&",
    )

    recent_date = max(
        [parse_iso_date(f.get("created_at", datetime.now().isoformat())) for f in nabla_data]
    )
    log.info("Most recent date scraped: %s", recent_date.isoformat())
    s3.put_object(
        Bucket=bucket_name,
        Key="max_date.txt",
        Body=recent_date.isoformat(),
    )