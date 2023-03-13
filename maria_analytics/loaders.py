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
        entity_name = entity or item["type"].replace(".", "/")
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
    req_field: str = "data",
    url_sep: str = "?",
    iterate: bool = True,
    save_to_s3: bool = True,
) -> list:
    req = re.get(url, headers=req_headers)
    req_json = req.json()
    data = req_json[req_field]

    s3 = get_client()

    if data:
        if save_to_s3:
            save_data(s3, bucket_name, data, entity)
        while req_json["has_more"] and iterate:
            cursor = req_json["next_cursor"]
            req = re.get(url + url_sep + "cursor=" + str(cursor), headers=req_headers)
            req_json = req.json()
            data_cursor = req_json[req_field]
            if save_to_s3:
                save_data(s3, bucket_name, data_cursor, entity)
            data += data_cursor

    return data


def parse_iso_date(date_value: str):
    return datetime.strptime(date_value.split(".")[0], "%Y-%m-%dT%H:%M:%S")


def get_incremental_nabla_data(
    url: str,
    bucket_name: str,
    req_headers: Dict[str, str] = headers,
):
    max_date_filename = "max_date.txt"
    s3 = get_client()
    s3_max_date_file = s3.list_objects_v2(
        Bucket=bucket_name, Prefix=max_date_filename, Delimiter="/"
    )
    if "Contents" in s3_max_date_file:
        last_date = (
            s3.get_object(Bucket=bucket_name, Key=max_date_filename)["Body"].read().decode("utf-8")
        )
    else:
        last_date = datetime(
            year=2020,
            month=1,
            day=1,
            hour=0,
            minute=0,
            second=0,
        ).isoformat()
        s3.put_object(
            Bucket=bucket_name,
            Key=max_date_filename,
            Body=last_date,
        )

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
        Key=max_date_filename,
        Body=recent_date.isoformat(),
    )


def get_files(s3, prefix: str, bucket_name: str) -> list:
    s3_result = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix, Delimiter="/")

    if "Contents" not in s3_result:
        return []

    file_list = []
    for key in s3_result["Contents"]:
        file_list.append(key["Key"])

    while s3_result["IsTruncated"]:
        continuation_key = s3_result["NextContinuationToken"]
        s3_result = s3.list_objects_v2(
            Bucket=bucket_name, Prefix=prefix, Delimiter="/", ContinuationToken=continuation_key
        )
        for key in s3_result["Contents"]:
            file_list.append(key["Key"])
    log.info("%s files found in %s", len(file_list), prefix)
    return file_list
