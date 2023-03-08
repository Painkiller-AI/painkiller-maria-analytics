import requests as re
from dotenv import load_dotenv
import os
import json
import boto3
aws_access_key_id = 'XXX'
aws_secret_access_key = 'XXX'
api_key_nabla = 'XXX'

s3 = boto3.client("s3", aws_access_key_id=aws_access_key_id,aws_secret_access_key=aws_secret_access_key)
def save_jsons(data):
    for item in data:
        file_name = '/'.join(item['type'].split('.'))+'/'+item['id']+'.json'
        s3.put_object(Bucket='nablalog', Key=file_name, Body=json.dumps(item))
api_key = api_key_nabla
headers={"Accept":"application/json",
'Authorization': f'Bearer {api_key}',
'Content-Type': 'application/json',}
last_date = s3.get_object(Bucket = 'nablalog', Key='max_date.txt')['Body'].read().decode('utf-8')
link_base = f"https://api.nabla.com/v1/server/webhook_events?created_at_gt={last_date}"
req=re.get(link_base,headers=headers).json()
data = req['data']
if len(data)>0:
    recent_date = req['data'][0]['created_at']
    s3.put_object(Bucket='nablalog', Key='max_date.txt', Body=recent_date)
    save_jsons(data)
    if req['has_more']:
        while True:
            cursor = req['next_cursor']
            print(cursor)
            url = link_base + '&cursor=' + str(cursor)
            req=re.get(url,headers=headers).json()
            data = req['data']
            save_jsons(data)
            if not req['has_more']:
                break

link_patients = f"https://api.nabla.com/v1/server/patients"
def save_patient(data):
    for patient in data:
        s3.put_object(Bucket='nablalog', Key=f"patients/{patient['id']}.json", Body=json.dumps(patient))
req=re.get(link_patients,headers=headers).json()
data = req['data']
if len(data)>0:
    save_patient(data)
    if req['has_more']:
        while True:
            cursor = req['next_cursor']
            url = link_patients + '?cursor=' + str(cursor)
            req=re.get(url,headers=headers).json()
            data = req['data']
            save_patient(data)
            if not req['has_more']:
                break

link_providers = f"https://api.nabla.com/v1/server/providers"
def save_providers(data):
    for patient in data:
        s3.put_object(Bucket='nablalog', Key=f"prodviders/{patient['id']}.json", Body=json.dumps(patient))
req=re.get(link_providers,headers=headers).json()
data = req['data']
if len(data)>0:
    save_providers(data)
    if req['has_more']:
        while True:
            cursor = req['next_cursor']
            url = link_providers + '?cursor=' + str(cursor)
            req=re.get(url,headers=headers).json()
            data = req['data']
            save_providers(data)
            if not req['has_more']:
                break