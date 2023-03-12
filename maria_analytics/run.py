import os
from io import StringIO
from pathlib import Path
from typing import Dict

import pandas as pd
from loaders import (
    get_client,
    get_files,
    get_incremental_nabla_data,
    get_nabla_data,
    get_s3_data,
)
from loggers import get_logger

BUCKET_NAME = os.getenv("BUCKET_NAME", "nablalog")
ANALYTICS_BUCKET_NAME = os.getenv("ANALYTICS_BUCKET_NAME", "nablaanalyticsv0")

log = get_logger(Path(__file__).stem)


def save_patients_analytics(
    bucket_name: str = BUCKET_NAME,
    analytics_bucket_name: str = ANALYTICS_BUCKET_NAME,
):
    s3 = get_client()
    s3_data = get_files(s3, "patients/", bucket_name)

    patient_data = [
        get_s3_data(s3, patient, bucket_name, ["id", "created_at", "date_of_birth", "sex"])
        for patient in s3_data
    ]
    patient_df = pd.DataFrame(patient_data)

    def medical_records(id, bucket_name: str = bucket_name):
        result = get_nabla_data(
            url=f"https://api.nabla.com/v1/server/patients/{id}/medical_data",
            bucket_name=bucket_name,
            req_field="total_count",
            iterate=False,
            save_to_s3=False,
        )
        return result

    patient_df["medical_records"] = patient_df["id"].apply(medical_records)

    csv_buf = StringIO()
    patient_df.to_csv(csv_buf, header=True, index=False)
    csv_buf.seek(0)
    s3.put_object(Bucket=analytics_bucket_name, Body=csv_buf.getvalue(), Key="patients.csv")
    log.info("Saved patients file to Analytics Bucket")


def save_providers_analytics(
    bucket_name: str = BUCKET_NAME,
    analytics_bucket_name: str = ANALYTICS_BUCKET_NAME,
):
    s3 = get_client()
    s3_data = get_files(s3, "prodviders/", bucket_name)

    provider_data = [
        get_s3_data(s3, provider, bucket_name, ["id", "title"]) for provider in s3_data
    ]
    provider_df = pd.DataFrame(provider_data)
    csv_buf = StringIO()
    provider_df.to_csv(csv_buf, header=True, index=False)
    csv_buf.seek(0)
    s3.put_object(Bucket=analytics_bucket_name, Body=csv_buf.getvalue(), Key="providers.csv")
    log.info("Saved providers file to Analytics Bucket")


def get_data_msg(s3, key: str, bucket_name: str) -> Dict[str, str]:
    result = get_s3_data(
        s3, key, bucket_name, fields=["id", "created_at", "data", "conversation_id"]
    )
    try:
        return {
            "id": result["id"],
            "created_at": result["created_at"],
            "author_id": result["data"]["author"]["id"],
            "author_type": result["data"]["author"]["type"],
            "conversation_id": result["data"]["conversation_id"],
        }
    except KeyError:
        pass


def save_messages_analytics(
    bucket_name: str = BUCKET_NAME,
    analytics_bucket_name: str = ANALYTICS_BUCKET_NAME,
):
    s3 = get_client()
    s3_data = get_files(s3, "conversation/message/created/", bucket_name)

    msg_data = [
        get_data_msg(
            s3,
            msg,
            bucket_name,
        )
        for msg in s3_data
    ]
    msg_data = [item for item in msg_data if item is not None]
    msg_df = pd.DataFrame(msg_data)
    csv_buf = StringIO()
    msg_df.to_csv(csv_buf, header=True, index=False)
    csv_buf.seek(0)
    s3.put_object(Bucket=analytics_bucket_name, Body=csv_buf.getvalue(), Key="messages.csv")
    log.info("Saved messages file to Analytics Bucket")


def get_data_video(s3, key: str, bucket_name: str):
    result = get_s3_data(s3, key, bucket_name, fields=["id", "data", "created_at"])
    try:
        return {
            "id": result["id"],
            "start_at": result["data"]["start_at"],
            "patient": result["data"]["patient"]["id"],
            "provider": result["data"]["provider"]["id"],
            "finish_at": result["created_at"],
        }
    except KeyError:
        pass


def save_video_analytics(
    bucket_name: str = BUCKET_NAME,
    analytics_bucket_name: str = ANALYTICS_BUCKET_NAME,
):
    s3 = get_client()
    s3_data = get_files(s3, "appointment/completed/", bucket_name)

    video_data = [get_data_video(s3, video, bucket_name) for video in s3_data]
    video_data = [item for item in video_data if item is not None]
    video_df = pd.DataFrame(video_data)
    csv_buf = StringIO()
    video_df.to_csv(csv_buf, header=True, index=False)
    csv_buf.seek(0)
    s3.put_object(Bucket=analytics_bucket_name, Body=csv_buf.getvalue(), Key="videos.csv")
    log.info("Saved videos file to Analytics Bucket")


def get_data_conversation(s3, key, bucket_name: str):
    result = get_s3_data(s3, key, bucket_name, ["data"])
    try:
        return {
            "conversation_id": result["data"]["id"],
            "patient_id": result["data"]["patients"][0]["id"],
        }
    except KeyError:
        pass


def save_conversation_analytics(
    bucket_name: str = BUCKET_NAME,
    analytics_bucket_name: str = ANALYTICS_BUCKET_NAME,
):
    s3 = get_client()
    s3_data = get_files(s3, "conversation/created/", bucket_name)

    conv_data = [get_data_conversation(s3, conv, bucket_name) for conv in s3_data]
    conv_data = [item for item in conv_data if item is not None]
    conv_df = pd.DataFrame(conv_data)
    csv_buf = StringIO()
    conv_df.to_csv(csv_buf, header=True, index=False)
    csv_buf.seek(0)
    s3.put_object(Bucket=analytics_bucket_name, Body=csv_buf.getvalue(), Key="conversations.csv")
    log.info("Saved conversation file to Analytics Bucket")


if __name__ == "__main__":
    log.info("Starting Nabla Analytical Extraction")
    log.info("Getting webhook events")
    get_incremental_nabla_data(
        url="https://api.nabla.com/v1/server/webhook_events?created_at_gt=",
        bucket_name=BUCKET_NAME,
    )
    log.info("Finished webhook events")
    log.info("Getting patients")
    get_nabla_data(
        url="https://api.nabla.com/v1/server/patients",
        bucket_name=BUCKET_NAME,
        entity="patients",
    )
    log.info("Finished patients")
    log.info("Getting providers")
    get_nabla_data(
        url="https://api.nabla.com/v1/server/providers",
        bucket_name=BUCKET_NAME,
        entity="providers",
    )
    log.info("Finished providers")
    save_patients_analytics()
    save_providers_analytics()
    save_messages_analytics()
    save_video_analytics()
    save_conversation_analytics()
