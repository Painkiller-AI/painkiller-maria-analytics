import json
import os
from io import StringIO
from itertools import compress

import boto3
import pandas as pd
import pygsheets
import requests as re

aws_access_key_id = os.environ["AWS_ACCESS_KEY_ID"]
aws_secret_access_key = os.environ["AWS_SECRET_ACCESS_KEY"]
api_key_nabla = os.environ["API_KEY_NABLA"]

gc = pygsheets.authorize(service_file="patbotupdate-d27e4be198ee.json")
sh = gc.open("Maria_analytics")

headers = {
    "Accept": "application/json",
    "Authorization": f"Bearer {api_key_nabla}",
    "Content-Type": "application/json",
}

s3 = boto3.client(
    "s3",
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
)


def get_files(bucket_name, prefix, s3):
    s3_result = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix, Delimiter="/")

    if "Contents" not in s3_result:
        return []

    file_list = []
    for key in s3_result["Contents"]:
        file_list.append(key["Key"])
    print(f"List count = {len(file_list)}")

    while s3_result["IsTruncated"]:
        continuation_key = s3_result["NextContinuationToken"]
        s3_result = s3.list_objects_v2(
            Bucket=bucket_name, Prefix=prefix, Delimiter="/", ContinuationToken=continuation_key
        )
        for key in s3_result["Contents"]:
            file_list.append(key["Key"])
        print(f"List count = {len(file_list)}")
    return file_list


def get_data_patient(key):
    result = s3.get_object(Bucket="nablalog", Key=key)
    result = result["Body"].read().decode()
    result = json.loads(result)
    select_itens = ["id", "created_at", "date_of_birth", "sex"]
    return dict((k, v) for k, v in result.items() if k in select_itens)


def get_data_provider(key):
    result = s3.get_object(Bucket="nablalog", Key=key)
    result = result["Body"].read().decode()
    result = json.loads(result)
    select_itens = ["id", "title"]
    return dict((k, v) for k, v in result.items() if k in select_itens)


def get_data_msg(key):
    result = s3.get_object(Bucket="nablalog", Key=key)
    result = result["Body"].read().decode()
    result = json.loads(result)
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


def get_data_video(key):
    result = s3.get_object(Bucket="nablalog", Key=key)
    result = result["Body"].read().decode()
    result = json.loads(result)
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


def get_data_conversation(key):
    result = s3.get_object(Bucket="nablalog", Key=key)
    result = result["Body"].read().decode()
    result = json.loads(result)
    try:
        return {
            "conversation_id": result["data"]["id"],
            "patient_id": result["data"]["patients"][0]["id"],
        }
    except KeyError:
        pass


def get_patients():
    patients_loaded = get_files("nablalog", "patients/", s3)
    patient_id_loaded = [item.split("/")[1].split(".")[0] for item in patients_loaded]
    patient_base = pd.read_csv(
        s3.get_object(Bucket="nablaanalyticsv0", Key="patients.csv")["Body"],
    )
    patient_id_loaded = [
        True if item not in patient_base.id.values else False for item in patient_id_loaded
    ]
    if sum(patient_id_loaded) > 0:
        patients_loaded = list(compress(patients_loaded, patient_id_loaded))
        patient_data = [get_data_patient(patient) for patient in patients_loaded]
        patient_data = pd.DataFrame(patient_data)

        def medical_records(id):
            link = f"https://api.nabla.com/v1/server/patients/{id}/medical_data"
            req = re.get(link, headers=headers).json()
            return req["total_count"]

        patient_data["medical_records"] = patient_data["id"].apply(lambda x: medical_records(x))
        patient_data = pd.concat([patient_base, patient_data], axis=0).reset_index(drop=True)
        csv_buf = StringIO()
        patient_data.to_csv(csv_buf, header=True, index=False)
        csv_buf.seek(0)
        s3.put_object(Bucket="nablaanalyticsv0", Body=csv_buf.getvalue(), Key="patients.csv")
        sh1 = sh._sheet_list[0]
        sh1.set_dataframe(patient_data, "A1")


def get_providers():
    providers_loaded = get_files("nablalog", "prodviders/", s3)
    providers_id_loaded = [item.split("/")[1].split(".")[0] for item in providers_loaded]
    providers_base = pd.read_csv(
        s3.get_object(Bucket="nablaanalyticsv0", Key="providers.csv")["Body"]
    )
    providers_id_loaded = [
        True if item not in providers_base.id.values else False for item in providers_id_loaded
    ]
    if sum(providers_id_loaded) > 0:
        providers_loaded = list(compress(providers_loaded, providers_id_loaded))
        provider_data = [get_data_provider(provider) for provider in providers_loaded]
        provider_data = pd.DataFrame(provider_data)
        provider_data = pd.concat([providers_base, provider_data], axis=0).reset_index(drop=True)
        csv_buf = StringIO()
        provider_data.to_csv(csv_buf, header=True, index=False)
        csv_buf.seek(0)
        s3.put_object(Bucket="nablaanalyticsv0", Body=csv_buf.getvalue(), Key="providers.csv")
        sh1 = sh._sheet_list[2]
        sh1.set_dataframe(provider_data, "A1")


def get_messages():
    msgs_loaded = get_files("nablalog", "conversation/message/created/", s3)
    msgs_id_loaded = [item.split("/")[-1].split(".")[0] for item in msgs_loaded]
    msg_base = pd.read_csv(s3.get_object(Bucket="nablaanalyticsv0", Key="messages.csv")["Body"])
    msgs_id_loaded = [True if item not in msg_base.id.values else False for item in msgs_id_loaded]
    if sum(msgs_id_loaded) > 0:
        msgs_loaded = list(compress(msgs_loaded, msgs_id_loaded))
        msg_data = [get_data_msg(msg) for msg in msgs_loaded]
        msg_data = [item for item in msg_data if item is not None]
        msg_data = pd.DataFrame(msg_data)
        msg_data = pd.concat([msg_base, msg_data], axis=0).reset_index(drop=True)
        csv_buf = StringIO()
        msg_data.to_csv(csv_buf, header=True, index=False)
        csv_buf.seek(0)
        s3.put_object(Bucket="nablaanalyticsv0", Body=csv_buf.getvalue(), Key="messages.csv")
        sh1 = sh._sheet_list[1]
        sh1.set_dataframe(msg_data, "A1")


def get_video():
    video_loaded = get_files("nablalog", "appointment/completed/", s3)
    video_id_loaded = [item.split("/")[-1].split(".")[0] for item in video_loaded]
    video_base = pd.read_csv(s3.get_object(Bucket="nablaanalyticsv0", Key="videos.csv")["Body"])
    video_id_loaded = [
        True if item not in video_base.id.values else False for item in video_id_loaded
    ]
    if sum(video_id_loaded) > 0:
        video_loaded = list(compress(video_loaded, video_id_loaded))
        video_data = [get_data_video(video) for video in video_loaded]
        video_data = [item for item in video_data if item is not None]
        video_data = pd.DataFrame(video_data)
        video_data = pd.concat([video_base, video_data], axis=0).reset_index(drop=True)
        csv_buf = StringIO()
        video_data.to_csv(csv_buf, header=True, index=False)
        csv_buf.seek(0)
        s3.put_object(Bucket="nablaanalyticsv0", Body=csv_buf.getvalue(), Key="videos.csv")
        sh1 = sh._sheet_list[3]
        sh1.set_dataframe(video_data, "A1")


def get_conversation():
    conv_loaded = get_files("nablalog", "conversation/created/", s3)
    conv_id_loaded = [item.split("/")[-1].split(".")[0] for item in conv_loaded]
    conv_base = pd.read_csv(
        s3.get_object(Bucket="nablaanalyticsv0", Key="conversations.csv")["Body"]
    )
    conv_id_loaded = [
        True if item not in conv_base.conversation_id.values else False for item in conv_id_loaded
    ]
    if sum(conv_id_loaded) > 0:
        conv_loaded = list(compress(conv_loaded, conv_id_loaded))
        conv_data = [get_data_conversation(conv) for conv in conv_loaded]
        conv_data = [item for item in conv_data if item is not None]
        conv_data = pd.DataFrame(conv_data)
        conv_data = pd.concat([conv_base, conv_data], axis=0).reset_index(drop=True)
        csv_buf = StringIO()
        conv_data.to_csv(csv_buf, header=True, index=False)
        csv_buf.seek(0)
        s3.put_object(Bucket="nablaanalyticsv0", Body=csv_buf.getvalue(), Key="conversations.csv")
        sh1 = sh._sheet_list[4]
        sh1.set_dataframe(conv_data, "A1")


get_patients()
get_providers()
get_messages()
get_video()
get_conversation()
