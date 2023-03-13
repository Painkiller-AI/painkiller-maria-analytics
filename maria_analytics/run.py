import os
from io import StringIO
from json import JSONDecodeError
from pathlib import Path

import pandas as pd
from loaders import get_client, get_files, get_incremental_nabla_data, get_nabla_data
from loggers import get_logger

BUCKET_NAME = os.getenv("BUCKET_NAME", "nablalog")
ANALYTICS_BUCKET_NAME = os.getenv("ANALYTICS_BUCKET_NAME", "nablaanalyticsv0")

log = get_logger(Path(__file__).stem)


def get_patients_medical_records_amount(
    bucket_name: str = BUCKET_NAME,
    analytics_bucket_name: str = ANALYTICS_BUCKET_NAME,
):
    s3 = get_client()
    s3_data = get_files(s3, "patients/", bucket_name)
    patients_id = [Path(f).stem for f in s3_data]

    patient_df = pd.DataFrame(data=patients_id, columns=["id"])

    def medical_records(id, bucket_name: str = bucket_name):
        try:
            result = get_nabla_data(
                url=f"https://api.nabla.com/v1/server/patients/{id}/medical_data",
                bucket_name=bucket_name,
                req_field="total_count",
                iterate=False,
                save_to_s3=False,
            )
        except JSONDecodeError:
            log.debug("Unknown patient found in history but was probably deleted")
            result = 0
        return result

    patient_df["medical_records"] = patient_df["id"].apply(medical_records)

    string_buffer = StringIO()
    patient_df.to_json(string_buffer, orient="records", lines=True)
    string_buffer.seek(0)
    s3.put_object(
        Bucket=analytics_bucket_name,
        Body=string_buffer.getvalue(),
        Key="medical_records/medical_records.json",
    )
    log.info("Saved medical_records file to Analytics Bucket")


def lambda_executor():
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
    get_patients_medical_records_amount()
    return {"statusCode": 200}


if __name__ == "__main__":
    lambda_executor()
